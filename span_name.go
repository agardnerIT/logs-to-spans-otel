// Copyright The OpenTelemetry Authors
// SPDX-License-Identifier: Apache-2.0

package logs_to_spans

import (
	"errors"
	"fmt"
	"strconv"
	"strings"
)

// defaultSpanNameTemplate is the span_name_template used when the option is
// absent. It reproduces the historical behaviour of naming the span after the
// full log body.
const defaultSpanNameTemplate = "{body}"

// spanNamePlaceholder identifies what a compiled template part reads from a log
// record. Parsing happens once, in Validate and in the factory, so rendering a
// name per record is a walk over a short slice with no error path.
type spanNamePlaceholder int

const (
	// spanNameLiteral is fixed text copied into every rendered name.
	spanNameLiteral spanNamePlaceholder = iota
	// spanNameBody expands to the full log body.
	spanNameBody
	// spanNameSeverity expands to the log record's severity text.
	spanNameSeverity
	// spanNameTruncatedBody expands to the first limit runes of the log body.
	spanNameTruncatedBody
)

// spanNamePart is one segment of a compiled span_name_template.
type spanNamePart struct {
	kind spanNamePlaceholder
	// text is the literal text for spanNameLiteral parts.
	text string
	// limit is the rune count for spanNameTruncatedBody parts.
	limit int
}

// spanNameTemplate is a parsed span_name_template, ready to render.
type spanNameTemplate struct {
	parts []spanNamePart
}

// compileSpanNameTemplate parses a span_name_template. Supported placeholders
// are {body}, {severity} and {truncated:N:body}; everything else is literal
// text. An unknown or malformed placeholder is an error so a typo surfaces at
// startup instead of naming every span with the raw template.
func compileSpanNameTemplate(tmpl string) (*spanNameTemplate, error) {
	if strings.TrimSpace(tmpl) == "" {
		return nil, errors.New("span_name_template must not be empty")
	}

	t := &spanNameTemplate{}
	rest := tmpl
	for {
		open := strings.IndexByte(rest, '{')
		if open < 0 {
			if rest != "" {
				t.parts = append(t.parts, spanNamePart{kind: spanNameLiteral, text: rest})
			}
			break
		}
		if open > 0 {
			t.parts = append(t.parts, spanNamePart{kind: spanNameLiteral, text: rest[:open]})
		}

		closeIdx := strings.IndexByte(rest[open:], '}')
		if closeIdx < 0 {
			return nil, fmt.Errorf("span_name_template has an unterminated placeholder: %q", rest[open:])
		}
		closeIdx += open

		part, err := parseSpanNamePlaceholder(rest[open+1 : closeIdx])
		if err != nil {
			return nil, fmt.Errorf("span_name_template: %w", err)
		}
		t.parts = append(t.parts, part)
		rest = rest[closeIdx+1:]
	}
	return t, nil
}

// parseSpanNamePlaceholder parses the text between a pair of braces. Whitespace
// around the tokens and around the count is ignored, so {truncated:100:body}
// and { truncated : 100 : body } are equivalent.
func parseSpanNamePlaceholder(expr string) (spanNamePart, error) {
	tokens := strings.Split(expr, ":")
	for i := range tokens {
		tokens[i] = strings.TrimSpace(tokens[i])
	}

	switch {
	case len(tokens) == 1 && tokens[0] == "body":
		return spanNamePart{kind: spanNameBody}, nil
	case len(tokens) == 1 && tokens[0] == "severity":
		return spanNamePart{kind: spanNameSeverity}, nil
	case len(tokens) == 3 && tokens[0] == "truncated" && tokens[2] == "body":
		n, err := strconv.Atoi(tokens[1])
		if err != nil {
			return spanNamePart{}, fmt.Errorf("placeholder %q: %q is not a whole number", "{"+expr+"}", tokens[1])
		}
		if n <= 0 {
			return spanNamePart{}, fmt.Errorf("placeholder %q: the character count must be greater than zero", "{"+expr+"}")
		}
		return spanNamePart{kind: spanNameTruncatedBody, limit: n}, nil
	default:
		return spanNamePart{}, fmt.Errorf(
			"unknown placeholder %q: supported placeholders are {body}, {severity} and {truncated:N:body}",
			"{"+expr+"}")
	}
}

// render expands the template for one log record.
func (t *spanNameTemplate) render(rec *logRecord) string {
	var b strings.Builder
	for _, part := range t.parts {
		switch part.kind {
		case spanNameBody:
			b.WriteString(rec.body)
		case spanNameSeverity:
			b.WriteString(rec.severity)
		case spanNameTruncatedBody:
			b.WriteString(truncateRunes(rec.body, part.limit))
		case spanNameLiteral:
			b.WriteString(part.text)
		}
	}
	return b.String()
}

// truncateRunes returns at most n runes from the start of s. It counts runes,
// not bytes, so a truncated name never ends in the middle of a multi-byte
// character.
func truncateRunes(s string, n int) string {
	if n <= 0 {
		return ""
	}
	count := 0
	for i := range s {
		if count == n {
			return s[:i]
		}
		count++
	}
	return s
}
