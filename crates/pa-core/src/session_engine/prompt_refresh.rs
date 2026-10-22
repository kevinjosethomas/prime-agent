//! Re-read prompt sources only when compaction starts a new cache prefix.

use crate::resources::{load_resources, LoadedResources, ResourceLoaderOptions};

pub(super) struct PromptSources {
    pub resource_options: ResourceLoaderOptions,
    pub messages_path: Option<String>,
    pub selected_tools: Vec<String>,
    pub allow_recursion: Option<bool>,
    pub rlm_depth: Option<u32>,
    pub daemonless: bool,
    pub can_spawn: bool,
    pub generic_mcp_servers: Vec<String>,
    pub mcp_manager: std::sync::Arc<std::sync::Mutex<crate::mcp::McpManager>>,
}

impl PromptSources {
    pub fn assemble(
        &self,
        resources: &LoadedResources,
        model_selector: &str,
        generic_mcp_servers: Vec<String>,
        vision_capable: Option<bool>,
    ) -> String {
        let model_prompts = crate::prompts::model_prompts::load_model_prompts(
            Some(model_selector),
            &self.resource_options.agent_dir,
        );
        crate::prompts::system_prompt::build_system_prompt(
            &crate::prompts::system_prompt::BuildSystemPromptOptions {
                custom_prompt: resources.system_prompt.clone(),
                model_prompt_extras: model_prompts.extras.as_deref(),
                vision_capable,
                cwd: self.resource_options.cwd.display().to_string(),
                messages_path: self.messages_path.clone(),
                context_files: resources
                    .agents_files
                    .iter()
                    .map(|file| (file.path.display().to_string(), file.content.clone()))
                    .collect(),
                skills: resources.skills.clone(),
                selected_tools: Some(self.selected_tools.iter().map(String::as_str).collect()),
                allow_recursion: self.allow_recursion,
                rlm_depth: self.rlm_depth,
                daemonless: self.daemonless,
                can_spawn: self.can_spawn,
                generic_mcp_servers,
                append_system_prompt: Some(resources.append_system_prompt.join("\n\n")),
                ..Default::default()
            },
        )
    }
}

impl super::AgentSession {
    /// The prompt pinned at the latest successful compaction, the
    /// latest `/reload` (or creation).
    ///
    /// # Panics
    /// Panics if the pinned-prompt lock is poisoned.
    #[must_use]
    pub fn pinned_system_prompt(&self) -> Option<String> {
        self.pinned_prompt.read().unwrap().clone()
    }

    /// Stage appended prompt sources for the next successful compaction.
    /// The current system prompt and conversation are left unchanged.
    ///
    /// # Panics
    /// Panics if the prompt-source lock is poisoned.
    pub fn set_prompt_guidelines(&self, sources: Vec<String>) {
        *self.prompt_guidelines.write().unwrap() = Some(sources);
    }

    pub(super) async fn build_current_system_prompt(
        &self,
        model: &pa_types::ai::Model,
    ) -> anyhow::Result<Option<String>> {
        Ok(self
            .reload_prompt_sources(model)
            .await?
            .map(|(_, prompt)| prompt))
    }

    /// Re-run the session's resource loader (the same roots and settings
    /// resolution the build used; the loader re-reads settings from disk)
    /// and assemble the current system prompt from what disk holds now.
    async fn reload_prompt_sources(
        &self,
        model: &pa_types::ai::Model,
    ) -> anyhow::Result<Option<(LoadedResources, String)>> {
        let Some(sources) = self.prompt_sources.clone() else {
            return Ok(None);
        };
        let guidelines = self.prompt_guidelines.read().unwrap().clone();
        let selector = format!("{}/{}", model.provider, model.id);
        let vision_capable = Some(model.input.contains(&pa_types::ai::ModelInput::Image));
        // File and package resolution must not block the worker's async lane.
        let (resources, prompt) = tokio::task::spawn_blocking(move || {
            let options = &sources.resource_options;
            let mut overrides = options.extra_builtin_skill_overrides.clone();
            overrides.extend(
                sources
                    .mcp_manager
                    .lock()
                    .unwrap()
                    .get_disabled_builtin_skill_overrides(),
            );
            let resources = load_resources(ResourceLoaderOptions {
                cwd: options.cwd.clone(),
                agent_dir: options.agent_dir.clone(),
                extra_builtin_skill_overrides: overrides,
                additional_skill_paths: options.additional_skill_paths.clone(),
                additional_prompt_paths: options.additional_prompt_paths.clone(),
                system_prompt: options.system_prompt.clone(),
                append_system_prompt: guidelines
                    .unwrap_or_else(|| options.append_system_prompt.clone()),
                ..Default::default()
            })?;
            let mut servers = sources.generic_mcp_servers.clone();
            servers.extend(
                sources
                    .mcp_manager
                    .lock()
                    .unwrap()
                    .get_enabled_persistent_generic_servers(),
            );
            let prompt = sources.assemble(&resources, &selector, servers, vision_capable);
            Ok::<_, anyhow::Error>((resources, prompt))
        })
        .await??;
        Ok(Some((resources, prompt)))
    }

    /// `/reload`'s skill half: re-run the session's resource loader and
    /// rebuild the skill inventory AND the system prompt from what disk
    /// holds now, so `/skill:` expansion, the resource snapshot, and
    /// `<available_skills>` match disk. The prompt changes ONLY here
    /// (and at compaction, as before): turns keep the built prompt, so
    /// the provider's prompt cache stays stable without an explicit
    /// reload. A session built without prompt sources reloads nothing.
    ///
    /// # Errors
    ///
    /// Returns the resource-loading failure; nothing is swapped (the
    /// inventory and prompt keep their previous state) when the disk
    /// re-read fails.
    ///
    /// # Panics
    ///
    /// Panics if a prompt or pinned-prompt lock is poisoned.
    pub async fn reload_skills_and_prompt(
        &self,
        model: &pa_types::ai::Model,
    ) -> anyhow::Result<()> {
        let Some((resources, prompt)) = self.reload_prompt_sources(model).await? else {
            return Ok(());
        };
        self.set_skills(resources.skills);
        self.set_skill_diagnostics(resources.skill_diagnostics);
        self.agent.set_system_prompt(prompt.clone()).await;
        *self.pinned_prompt.write().unwrap() = Some(prompt);
        Ok(())
    }
}
