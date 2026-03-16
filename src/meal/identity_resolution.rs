use std::collections::HashMap;

pub fn normalize_participant_key(input: &str) -> String {
    input
        .chars()
        .filter(|ch| ch.is_ascii_alphanumeric() || *ch == '_')
        .collect::<String>()
        .to_ascii_lowercase()
}

pub fn resolve_distilled_user_id(
    extracted_user_id: &str,
    source_sender: Option<&str>,
    known_participants: &[String],
) -> Option<String> {
    let mut by_norm = HashMap::new();
    for participant in known_participants {
        let key = normalize_participant_key(participant);
        if key.is_empty() {
            continue;
        }
        by_norm.entry(key).or_insert_with(|| participant.clone());
    }

    let extracted_norm = normalize_participant_key(extracted_user_id);
    if extracted_norm.is_empty() {
        return None;
    }

    if let Some(canonical) = by_norm.get(extracted_norm.as_str()) {
        return Some(canonical.clone());
    }

    // Only map through source sender when it is an exact normalized match.
    if let Some(sender) = source_sender {
        let sender_norm = normalize_participant_key(sender);
        if !sender_norm.is_empty() && sender_norm == extracted_norm {
            if let Some(canonical) = by_norm.get(sender_norm.as_str()) {
                return Some(canonical.clone());
            }
        }
    }

    None
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn does_not_map_to_other_participant_when_ambiguous() {
        let known = vec!["sidjain26".to_string(), "mimosa0807".to_string()];
        let resolved = resolve_distilled_user_id("Shreya", Some("sidjain26"), &known);
        assert!(
            resolved.is_none(),
            "ambiguous references should remain unresolved"
        );
    }

    #[test]
    fn resolves_exact_known_participant_match() {
        let known = vec!["sidjain26".to_string(), "mimosa0807".to_string()];
        let resolved = resolve_distilled_user_id("mimosa0807", Some("sidjain26"), &known);
        assert_eq!(resolved.as_deref(), Some("mimosa0807"));
    }
}
