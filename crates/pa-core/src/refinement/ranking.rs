//! Harness relevance ranking: query terms and per-entry scoring.

use std::collections::HashMap;

use super::{HarnessEntry, RefinementKind};

/// Term -> weight, built from task signal (goal, recent messages).
pub type HarnessQueryTerms = HashMap<String, f64>;

/// Inverse document frequency per query term over the entries being ranked:
/// `ln(1 + documents / matches)` — rare distinctive terms outrank ubiquitous
/// ones; terms matching no entry are absent (they cannot score anything).
#[must_use]
pub fn harness_query_term_idf(
    entries: &[HarnessEntry],
    terms: &HarnessQueryTerms,
) -> HarnessQueryTerms {
    let mut idf = HarnessQueryTerms::new();
    if terms.is_empty() {
        return idf;
    }
    let mut matches: HashMap<&str, usize> = HashMap::new();
    for entry in entries {
        let title = entry.title.to_lowercase();
        let content = entry.content.to_lowercase();
        let identifier = format!("{} {}", entry.path.to_lowercase(), entry.id.to_lowercase());
        for term in terms.keys() {
            if title.contains(term.as_str())
                || content.contains(term.as_str())
                || identifier.contains(term.as_str())
            {
                *matches.entry(term.as_str()).or_insert(0) += 1;
            }
        }
    }
    for (term, document_frequency) in matches {
        idf.insert(
            term.to_string(),
            (1.0 + entries.len() as f64 / document_frequency as f64).ln(),
        );
    }
    idf
}

/// Score one entry against query terms: weighted per-term overlap across
/// title/content/identifier fields (coverage weighted, not repetition),
/// each term discounted by its document frequency in the ranked corpus
/// (`idf`; a missing map weights every term at 1).
#[must_use]
pub fn score_harness_entry_for_query(
    entry: &HarnessEntry,
    terms: &HarnessQueryTerms,
    idf: Option<&HarnessQueryTerms>,
) -> f64 {
    if terms.is_empty() {
        return 0.0;
    }
    let title = entry.title.to_lowercase();
    let content = entry.content.to_lowercase();
    let identifier = format!("{} {}", entry.path.to_lowercase(), entry.id.to_lowercase());
    let mut score = 0.0;
    for (term, weight) in terms {
        let mut fields = 0;
        if title.contains(term.as_str()) {
            fields += 1;
        }
        if content.contains(term.as_str()) {
            fields += 1;
        }
        if identifier.contains(term.as_str()) {
            fields += 1;
        }
        if fields > 0 {
            let term_idf = idf
                .and_then(|map| map.get(term.as_str()).copied())
                .unwrap_or(1.0);
            score += weight * term_idf * (1.0 + f64::from(fields - 1) * 0.5);
        }
    }
    score
}

#[cfg(test)]
mod tests {
    use super::super::{empty_harness_state, HarnessScope};
    use super::*;


    fn make_entry(id: &str, title: &str, content: &str, path: &str) -> HarnessEntry {
        HarnessEntry {
            id: id.to_string(),
            kind: RefinementKind::Memory,
            title: title.to_string(),
            content: content.to_string(),
            path: path.to_string(),
            scope: Some(HarnessScope::Global),
            reference: serde_json::Map::default(),
            arguments: serde_json::Map::default(),
            metadata: serde_json::Map::default(),
            source: "test".to_string(),
            created_at: String::new(),
            updated_at: String::new(),
            version: 1,
        }
    }

    #[test]
    fn scoring_field_coverage() {
        let entry = HarnessEntry {
            id: "web".to_string(),
            kind: RefinementKind::Skill,
            title: "Web Search".to_string(),
            content: "searches the web for results".to_string(),
            path: "/skills/web".to_string(),
            scope: Some(HarnessScope::Global),
            reference: serde_json::Map::default(),
            arguments: serde_json::Map::default(),
            metadata: serde_json::Map::default(),
            source: "test".to_string(),
            created_at: String::new(),
            updated_at: String::new(),
            version: 1,
        };
        let mut terms = HarnessQueryTerms::new();
        terms.insert("web".to_string(), 1.0);
        // web matches title + content + identifier: 3 fields -> 1.0*(1+1.0) = 2.
        let score = score_harness_entry_for_query(&entry, &terms, None);
        assert!((score - 2.0).abs() < 1e-9);
        terms.insert("absent".to_string(), 1.0);
        assert!((score_harness_entry_for_query(&entry, &terms, None) - score).abs() < 1e-9);
    }

    #[test]
    fn idf_discounts_common_terms_over_the_ranked_corpus() {
        let terms = HarnessQueryTerms::from_iter([
            ("session".to_string(), 1.0),
            ("quantum".to_string(), 1.0),
            ("missing".to_string(), 1.0),
        ]);
        let common0 = make_entry("common0", "Session notes", "session text", "general");
        let common1 = make_entry("common1", "Session notes", "session text", "general");
        let rare = make_entry("rare", "Quantum note", "quantum text", "general");
        // "session" matches 2 of 3 entries, "quantum" 1 of 3, "missing" none.
        let corpus = [common0.clone(), common1.clone(), rare.clone()];
        let idf = harness_query_term_idf(&corpus, &terms);
        assert_eq!(idf.len(), 2);
        let documents: f64 = 3.0;
        assert!((idf["session"] - (1.0 + documents / 2.0).ln()).abs() < 1e-9);
        assert!((idf["quantum"] - (1.0 + documents / 1.0).ln()).abs() < 1e-9);
        // The discount scales the weighted overlap: "quantum" covers 2 fields of 1 entry.
        let rare_score = score_harness_entry_for_query(&rare, &terms, Some(&idf));
        assert!((rare_score - (1.0 + documents / 1.0).ln() * 1.5).abs() < 1e-9);
        // A term in every entry still weighs ln(2); degenerate corpora stay
        // inert, and empty terms or corpora score nothing.
        let solo_idf = harness_query_term_idf(std::slice::from_ref(&rare), &terms);
        assert!((solo_idf["quantum"] - 2.0_f64.ln()).abs() < 1e-9);
        assert!(harness_query_term_idf(&[], &terms).is_empty());
        assert!(harness_query_term_idf(&corpus, &HarnessQueryTerms::new()).is_empty());
    }




}
