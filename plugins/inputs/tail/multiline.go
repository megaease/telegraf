package tail

import (
	"bytes"
	"errors"
	"regexp"
	"strings"
	"time"

	"github.com/influxdata/telegraf/config"
)

// Indicates relation to the multiline event: previous or next
type MultilineMatchWhichLine int

type Multiline struct {
	config               *MultilineConfig
	enabled              bool
	replacePatternRegexp *regexp.Regexp
	startPatternRegexp   *regexp.Regexp
	endPatternRegexp     *regexp.Regexp
	patternRegexp        *regexp.Regexp
	quote                byte
	inQuote              bool

	startedMatch bool
}

type MultilineConfig struct {
	ReplacePattern string `toml:"replace_pattern"`
	Replacement    string `toml:"replacement"`

	StartPattern    string                  `toml:"start_pattern"`
	Pattern         string                  `toml:"pattern"`
	EndPattern      string                  `toml:"end_pattern"`
	MatchWhichLine  MultilineMatchWhichLine `toml:"match_which_line"`
	InvertMatch     bool                    `toml:"invert_match"`
	PreserveNewline bool                    `toml:"preserve_newline"`
	Quotation       string                  `toml:"quotation"`
	Timeout         *config.Duration        `toml:"timeout"`
}

const (
	// Previous => Append current line to previous line
	Previous MultilineMatchWhichLine = iota
	// Next => Next line will be appended to current line
	Next
)

func (m *MultilineConfig) NewMultiline() (*Multiline, error) {
	var replacePatternRegexp *regexp.Regexp
	var startPatternRegexp *regexp.Regexp
	var endPatternRegexp *regexp.Regexp
	var patternRegexp *regexp.Regexp

	if m.ReplacePattern != "" {
		var err error
		if replacePatternRegexp, err = regexp.Compile(m.ReplacePattern); err != nil {
			return nil, errors.New("replace_pattern is invalid")
		}
	}

	if m.StartPattern != "" {
		if m.EndPattern == "" {
			return nil, errors.New("start_pattern is set, but end_pattern is not")
		}

		var err error
		if startPatternRegexp, err = regexp.Compile(m.StartPattern); err != nil {
			return nil, err
		}
	}

	if m.EndPattern != "" {
		if m.StartPattern == "" {
			return nil, errors.New("end_pattern is set, but start_pattern is not")
		}

		var err error
		if endPatternRegexp, err = regexp.Compile(m.EndPattern); err != nil {
			return nil, err
		}
	}

	if m.Pattern != "" {
		var err error
		if patternRegexp, err = regexp.Compile(m.Pattern); err != nil {
			return nil, err
		}
	}

	var quote byte
	switch m.Quotation {
	case "", "ignore":
		m.Quotation = "ignore"
	case "single-quotes":
		quote = '\''
	case "double-quotes":
		quote = '"'
	case "backticks":
		quote = '`'
	default:
		return nil, errors.New("invalid 'quotation' setting")
	}

	enabled := m.Pattern != "" || quote != 0 || m.StartPattern != "" || m.EndPattern != "" || m.ReplacePattern != ""
	if m.Timeout == nil || time.Duration(*m.Timeout).Nanoseconds() == int64(0) {
		d := config.Duration(5 * time.Second)
		m.Timeout = &d
	}

	return &Multiline{
		config:               m,
		enabled:              enabled,
		replacePatternRegexp: replacePatternRegexp,
		startPatternRegexp:   startPatternRegexp,
		endPatternRegexp:     endPatternRegexp,
		patternRegexp:        patternRegexp,
		quote:                quote,
	}, nil
}

func (m *Multiline) IsEnabled() bool {
	return m.enabled
}

func (m *Multiline) ProcessLine(text string, buffer *bytes.Buffer) string {
	if m.replacePatternRegexp != nil {
		text = m.replacePatternRegexp.ReplaceAllString(text, m.config.Replacement)
	}

	if m.startPatternRegexp != nil && m.endPatternRegexp != nil {
		if m.startedMatch {
			if m.endPatternRegexp.MatchString(text) {
				// End pattern found, flush the buffer.
				if m.config.PreserveNewline {
					buffer.WriteString("\n")
				}
				buffer.WriteString(text)
				text = buffer.String()
				buffer.Reset()

				m.startedMatch = false

				// fmt.Printf("### match %s : %s\n\n", m.endPatternRegexp.String(), text)

				return text
			}

			// Just collect the text between start and end patterns.
			if buffer.Len() > 0 && m.config.PreserveNewline {
				buffer.WriteString("\n")
			}
			buffer.WriteString(text)
			return ""
		}

		// fmt.Printf("--- match %s : %s\n", m.startPatternRegexp.String(), text)
		// fmt.Printf("--- result: %v\n\n", m.startPatternRegexp.MatchString(text))
		// Start pattern found, start collecting the text.
		if m.startPatternRegexp.MatchString(text) {
			m.startedMatch = true

			previousText := buffer.String()
			buffer.Reset()
			buffer.WriteString(text)

			return previousText
		}

		// if we are not in a match, just return the text
		return text
	}

	if m.matchQuotation(text) || m.matchString(text) {
		// Restore the newline removed by tail's scanner
		if buffer.Len() > 0 && m.config.PreserveNewline {
			buffer.WriteString("\n")
		}
		buffer.WriteString(text)
		return ""
	}

	if m.config.MatchWhichLine == Previous {
		previousText := buffer.String()
		buffer.Reset()
		buffer.WriteString(text)
		text = previousText
	} else {
		// Next
		if buffer.Len() > 0 {
			if m.config.PreserveNewline {
				buffer.WriteString("\n")
			}
			buffer.WriteString(text)
			text = buffer.String()
			buffer.Reset()
		}
	}

	return text
}

func (m *Multiline) Flush(buffer *bytes.Buffer) string {
	if buffer.Len() == 0 {
		return ""
	}
	text := buffer.String()
	buffer.Reset()
	return text
}

func (m *Multiline) matchQuotation(text string) bool {
	if m.config.Quotation == "ignore" {
		return false
	}
	escaped := 0
	count := 0
	for i := 0; i < len(text); i++ {
		if text[i] == '\\' {
			escaped++
			continue
		}

		// If we do encounter a backslash-quote combination, we interpret this
		// as an escaped-quoted and should not count the quote. However,
		// backslash-backslash combinations (or any even number of backslashes)
		// are interpreted as a literal backslash not escaping the quote.
		if text[i] == m.quote && escaped%2 == 0 {
			count++
		}
		// If we encounter any non-quote, non-backslash character we can
		// safely reset the escape state.
		escaped = 0
	}
	even := count%2 == 0
	m.inQuote = (m.inQuote && even) || (!m.inQuote && !even)
	return m.inQuote
}

func (m *Multiline) matchStartLine(text string) bool {
	if m.startPatternRegexp != nil {
		// start pattern does not consider invert match option.
		return m.startPatternRegexp.MatchString(text)
	}
	return false
}

func (m *Multiline) matchEndLine(text string) bool {
	if m.endPatternRegexp != nil {
		// end pattern does not consider invert match option.
		return m.endPatternRegexp.MatchString(text)
	}
	return false
}

func (m *Multiline) matchString(text string) bool {
	if m.patternRegexp != nil {
		return m.patternRegexp.MatchString(text) != m.config.InvertMatch
	}
	return false
}

func (w MultilineMatchWhichLine) String() string {
	switch w {
	case Previous:
		return "previous"
	case Next:
		return "next"
	}
	return ""
}

// UnmarshalTOML implements ability to unmarshal MultilineMatchWhichLine from TOML files.
func (w *MultilineMatchWhichLine) UnmarshalTOML(data []byte) (err error) {
	return w.UnmarshalText(data)
}

// UnmarshalText implements encoding.TextUnmarshaler
func (w *MultilineMatchWhichLine) UnmarshalText(data []byte) (err error) {
	s := string(data)
	switch strings.ToUpper(s) {
	case `PREVIOUS`, `"PREVIOUS"`, `'PREVIOUS'`:
		*w = Previous
		return nil

	case `NEXT`, `"NEXT"`, `'NEXT'`:
		*w = Next
		return nil
	}
	*w = -1
	return errors.New("unknown multiline MatchWhichLine")
}

// MarshalText implements encoding.TextMarshaler
func (w MultilineMatchWhichLine) MarshalText() ([]byte, error) {
	s := w.String()
	if s != "" {
		return []byte(s), nil
	}
	return nil, errors.New("unknown multiline MatchWhichLine")
}
