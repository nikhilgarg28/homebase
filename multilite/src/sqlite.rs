//! Shared SQLite syntax and catalog utilities.

/// Render one arbitrary SQLite identifier without changing its spelling.
pub(crate) fn quote_identifier(identifier: &str) -> String {
    format!("\"{}\"", identifier.replace('"', "\"\""))
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn identifier_quoting_preserves_spelling_and_escapes_quotes() {
        assert_eq!(quote_identifier("notes"), "\"notes\"");
        assert_eq!(quote_identifier("Mixed Case"), "\"Mixed Case\"");
        assert_eq!(quote_identifier("say \"hi\""), "\"say \"\"hi\"\"\"");
        assert_eq!(quote_identifier(""), "\"\"");
    }
}
