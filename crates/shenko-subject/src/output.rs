//! Decode once at the model-result boundary; consumers never inspect raw text.
//! Controls are whole unindented lines outside Markdown fences/quotes. There is
//! no natural-language trigger, legacy alias, or raw-publication fallback.
use pulldown_cmark::{Event, Parser, Tag};
use shenko_identity::IdentityService;
use shenko_types::{
    error::{ErrorScope, ProtocolError, RetryStrategy, ShenkoError},
    ids::EntityId,
    object::ObjectRef,
    output::*,
};

pub const OUTPUT_CONTRACT: &str = "控制动作只在独立、无缩进、非引用/代码块的一行生效：\n\
[reply:group:群号] 正文 或 [reply:private:QQ号] 正文，后续普通行续接正文；\n\
[remember] 要保存的内容；[delegate] 有限子任务的完整目标与提示。\n\
身份控制由身份能力说明提供。控制行不是可发布正文；引用/演示请放代码块或引用块。\n\
只支持此协议；[memory]、dispatch: 等旧伪协议不执行。";

fn invalid(line: u64, reason: &str) -> ShenkoError {
    ProtocolError::new(
        "CONTROL_PROTOCOL_INVALID",
        ErrorScope::Request,
        RetryStrategy::NewRevision,
    )
    .with_details(serde_json::json!({"line":line,"reason":reason,"protocol":1}))
    .into()
}

/// The caller has already archived the complete original output at raw_ref.
pub fn parse_output(
    step_id: EntityId,
    raw_ref: ObjectRef,
    raw: &str,
    identity: &IdentityService,
) -> Result<ParsedOutput, ShenkoError> {
    if raw_ref.bytes != raw.len() as u64
        || raw_ref.digest != shenko_types::object::Digest::blake3(raw.as_bytes())
        || raw_ref.locator.is_empty()
    {
        return Err(ProtocolError::new(
            "MODEL_OUTPUT_SOURCE_MISMATCH",
            ErrorScope::Request,
            RetryStrategy::NeverAutomatically,
        )
        .into());
    }

    let mut result = ParsedOutput {
        schema_version: 1,
        step_id,
        raw_ref,
        text: String::new(),
        controls: vec![],
    };
    let mut offset = 0;
    let mut reply: Option<usize> = None;
    // CommonMark source ranges include lazy blockquote/list continuations and
    // resolved shortcut reference links. A line-prefix heuristic cannot tell
    // these from real controls. Never render/rewrite the original evidence.
    let mut literal_ranges: Vec<_> = Parser::new(raw)
        .into_offset_iter()
        .filter_map(|(event, range)| match event {
            Event::Start(
                Tag::BlockQuote(_)
                | Tag::List(_)
                | Tag::CodeBlock(_)
                | Tag::HtmlBlock
                | Tag::Link { .. }
                | Tag::Image { .. },
            )
            | Event::Code(_) => Some(range),
            _ => None,
        })
        .collect();
    literal_ranges.sort_by_key(|range| range.start);
    let mut literal_ranges = literal_ranges.into_iter().peekable();
    let mut identity_fields = std::collections::HashSet::new();
    for (index, chunk) in raw.split_inclusive('\n').enumerate() {
        let line_no = index as u64 + 1;
        let line = chunk.trim_end_matches(['\r', '\n']);
        let span = OutputSpan {
            start: offset,
            end: offset + chunk.len(),
            line: line_no,
        };
        offset += chunk.len();
        while literal_ranges
            .peek()
            .is_some_and(|range| range.end <= span.start)
        {
            literal_ranges.next();
        }
        let active = !line.starts_with([' ', '\t'])
            && !literal_ranges
                .peek()
                .is_some_and(|range| range.contains(&span.start));
        // ASCII control tags are reserved at column zero. Non-ASCII prose,
        // references, indentation, quotes and fenced examples stay literal.
        let tag = line
            .strip_prefix('[')
            .and_then(|v| v.split_once(']').map(|(h, _)| h));
        let markdown_reference = line
            .split_once(']')
            .is_some_and(|(_, suffix)| suffix.starts_with(['(', '[', ':']));
        let control = active
            && !markdown_reference
            && (tag.is_some_and(|h| {
                h.bytes().next().is_some_and(|c| c.is_ascii_alphabetic())
                    && h.bytes()
                        .all(|c| c.is_ascii_alphanumeric() || b":_-".contains(&c))
            }) || (tag.is_none()
                && line
                    .strip_prefix('[')
                    .is_some_and(|v| v.bytes().next().is_some_and(|c| c.is_ascii_lowercase())))
                || line.starts_with("dispatch:"));
        if control {
            if line.starts_with("[memory]") || line.starts_with("dispatch:") {
                return Err(invalid(
                    line_no,
                    "obsolete control protocol; use [remember] / [delegate]",
                ));
            }
            let (header, value) = line
                .split_once(']')
                .ok_or_else(|| invalid(line_no, "missing ]"))?;
            let value = value.strip_prefix(' ').unwrap_or(value);
            let action = if let Some(target) = header.strip_prefix("[reply:") {
                let (kind, id) = target
                    .split_once(':')
                    .ok_or_else(|| invalid(line_no, "reply target must include kind and id"))?;
                let id = id
                    .parse::<u64>()
                    .ok()
                    .filter(|id| *id > 0)
                    .ok_or_else(|| invalid(line_no, "reply id must be positive"))?;
                let target = match kind {
                    "group" => PublicationTarget::QqGroup(id),
                    "private" => PublicationTarget::QqPrivate(id),
                    _ => return Err(invalid(line_no, "unknown reply target")),
                };
                let text = if value.is_empty() {
                    String::new()
                } else {
                    format!("{value}{}", &chunk[line.len()..])
                };
                result.text.push_str(&text);
                reply = Some(result.controls.len());
                OutputAction::Reply { target, text }
            } else if let Some(change) =
                identity.parse_control(header.trim_start_matches('['), value)
            {
                if !identity_fields.insert(std::mem::discriminant(&change)) {
                    return Err(invalid(line_no, "duplicate identity field in one result"));
                }
                if value.trim().is_empty() {
                    return Err(invalid(line_no, "empty identity change"));
                }
                OutputAction::Identity { change }
            } else {
                if value.trim().is_empty() {
                    return Err(invalid(line_no, "empty action"));
                }
                match header {
                    "[remember" => OutputAction::Remember {
                        text: value.to_owned(),
                    },
                    "[delegate" => OutputAction::Delegate {
                        text: value.to_owned(),
                    },
                    _ => return Err(invalid(line_no, "unknown control")),
                }
            };
            result.controls.push(OutputControl {
                action_id: EntityId::new(),
                spans: vec![span],
                action,
            });
        } else {
            result.text.push_str(chunk);
            if let Some(index) = reply {
                let control = &mut result.controls[index];
                if let OutputAction::Reply { text, .. } = &mut control.action {
                    text.push_str(chunk);
                }
                control.spans.push(span);
            }
        }
    }
    Ok(result)
}
