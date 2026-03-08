import { useState, useEffect } from 'react'

const strategyFiles = import.meta.glob('/src/strategy/*.md', { query: '?raw', import: 'default' })

interface StrategyEntry {
  filename: string
  date: string
  content: string
}

export default function Strategy() {
  const [entries, setEntries] = useState<StrategyEntry[]>([])
  const [loading, setLoading] = useState(true)

  useEffect(() => {
    async function load() {
      const results: StrategyEntry[] = []
      for (const [path, loader] of Object.entries(strategyFiles)) {
        const content = await loader() as string
        const filename = path.split('/').pop()!.replace('.md', '')
        results.push({ filename, date: filename, content })
      }
      results.sort((a, b) => b.date.localeCompare(a.date))
      setEntries(results)
      setLoading(false)
    }
    load()
  }, [])

  if (loading) return <div className="py-12 text-center text-earth-400">Loading strategy docs...</div>

  if (entries.length === 0) {
    return (
      <div>
        <h1 className="text-2xl font-bold text-earth-900 mb-1">Strategy</h1>
        <p className="text-sm text-earth-500 mb-6">No strategy documents found. Add markdown files to <code className="text-xs bg-earth-100 px-1.5 py-0.5 rounded">frontend/src/strategy/</code></p>
      </div>
    )
  }

  return (
    <div>
      <h1 className="text-2xl font-bold text-earth-900 mb-1">Strategy</h1>
      <p className="text-sm text-earth-500 mb-6">Strategic roadmap and session notes</p>

      <div className="space-y-6">
        {entries.map((entry) => (
          <article key={entry.filename} className="bg-white rounded-lg border border-earth-200 overflow-hidden">
            <div className="px-6 py-3 bg-earth-50 border-b border-earth-200 flex items-center justify-between">
              <h2 className="font-semibold text-earth-800">{entry.date}</h2>
            </div>
            <div className="px-6 py-5 prose prose-sm prose-earth max-w-none">
              <MarkdownContent content={entry.content} />
            </div>
          </article>
        ))}
      </div>
    </div>
  )
}

function MarkdownContent({ content }: { content: string }) {
  // Simple markdown rendering — handles headers, lists, tables, code, bold, links
  const lines = content.split('\n')
  const elements: React.ReactNode[] = []
  let i = 0
  let key = 0

  while (i < lines.length) {
    const line = lines[i]

    // Skip empty lines
    if (line.trim() === '') { i++; continue }

    // Horizontal rule
    if (/^---+$/.test(line.trim())) {
      elements.push(<hr key={key++} className="my-6 border-earth-200" />)
      i++
      continue
    }

    // Headers
    const headerMatch = line.match(/^(#{1,6})\s+(.+)/)
    if (headerMatch) {
      const level = headerMatch[1].length
      const text = renderInline(headerMatch[2])
      const cls = level === 1 ? 'text-xl font-bold text-earth-900 mt-6 mb-3'
        : level === 2 ? 'text-lg font-semibold text-earth-900 mt-5 mb-2'
        : level === 3 ? 'text-base font-semibold text-earth-800 mt-4 mb-2'
        : 'text-sm font-semibold text-earth-700 mt-3 mb-1'
      elements.push(<div key={key++} className={cls}>{text}</div>)
      i++
      continue
    }

    // Table
    if (line.includes('|') && i + 1 < lines.length && lines[i + 1].includes('---')) {
      const tableLines: string[] = []
      while (i < lines.length && lines[i].includes('|')) {
        tableLines.push(lines[i])
        i++
      }
      elements.push(<MarkdownTable key={key++} lines={tableLines} />)
      continue
    }

    // Unordered list
    if (/^[-*]\s/.test(line.trim())) {
      const items: string[] = []
      while (i < lines.length && /^[-*]\s/.test(lines[i].trim())) {
        items.push(lines[i].trim().replace(/^[-*]\s+/, ''))
        i++
      }
      elements.push(
        <ul key={key++} className="list-disc list-inside space-y-1 text-sm text-earth-700 my-2">
          {items.map((item, j) => <li key={j}>{renderInline(item)}</li>)}
        </ul>
      )
      continue
    }

    // Ordered list
    if (/^\d+\.\s/.test(line.trim())) {
      const items: string[] = []
      while (i < lines.length && /^\d+\.\s/.test(lines[i].trim())) {
        items.push(lines[i].trim().replace(/^\d+\.\s+/, ''))
        i++
      }
      elements.push(
        <ol key={key++} className="list-decimal list-inside space-y-1 text-sm text-earth-700 my-2">
          {items.map((item, j) => <li key={j}>{renderInline(item)}</li>)}
        </ol>
      )
      continue
    }

    // Checkbox items
    if (/^-\s*\[[ x]\]/.test(line.trim())) {
      const items: { checked: boolean; text: string }[] = []
      while (i < lines.length && /^-\s*\[[ x]\]/.test(lines[i].trim())) {
        const m = lines[i].trim().match(/^-\s*\[([ x])\]\s*(.*)/)
        if (m) items.push({ checked: m[1] === 'x', text: m[2] })
        i++
      }
      elements.push(
        <ul key={key++} className="space-y-1 text-sm text-earth-700 my-2">
          {items.map((item, j) => (
            <li key={j} className="flex items-start gap-2">
              <span className={`mt-0.5 ${item.checked ? 'text-emerald-600' : 'text-earth-400'}`}>
                {item.checked ? '✓' : '○'}
              </span>
              <span className={item.checked ? 'line-through text-earth-400' : ''}>{renderInline(item.text)}</span>
            </li>
          ))}
        </ul>
      )
      continue
    }

    // Paragraph
    elements.push(<p key={key++} className="text-sm text-earth-700 my-2 leading-relaxed">{renderInline(line)}</p>)
    i++
  }

  return <>{elements}</>
}

function renderInline(text: string): React.ReactNode {
  // Split on bold, code, and links
  const parts: React.ReactNode[] = []
  let remaining = text
  let k = 0

  while (remaining.length > 0) {
    // Bold
    const boldMatch = remaining.match(/\*\*(.+?)\*\*/)
    // Code
    const codeMatch = remaining.match(/`([^`]+)`/)
    // Link
    const linkMatch = remaining.match(/\[([^\]]+)\]\(([^)]+)\)/)

    // Find earliest match
    const matches = [
      boldMatch && { type: 'bold', index: boldMatch.index!, match: boldMatch },
      codeMatch && { type: 'code', index: codeMatch.index!, match: codeMatch },
      linkMatch && { type: 'link', index: linkMatch.index!, match: linkMatch },
    ].filter(Boolean).sort((a, b) => a!.index - b!.index)

    if (matches.length === 0) {
      parts.push(remaining)
      break
    }

    const first = matches[0]!
    if (first.index > 0) {
      parts.push(remaining.slice(0, first.index))
    }

    if (first.type === 'bold') {
      parts.push(<strong key={k++} className="font-semibold text-earth-900">{first.match[1]}</strong>)
    } else if (first.type === 'code') {
      parts.push(<code key={k++} className="text-xs bg-earth-100 px-1.5 py-0.5 rounded font-mono text-wine-700">{first.match[1]}</code>)
    } else if (first.type === 'link') {
      parts.push(<span key={k++} className="text-wine-600">{first.match[1]}</span>)
    }

    remaining = remaining.slice(first.index + first.match[0].length)
  }

  return parts.length === 1 && typeof parts[0] === 'string' ? parts[0] : <>{parts}</>
}

function MarkdownTable({ lines }: { lines: string[] }) {
  const parseRow = (line: string) =>
    line.split('|').map(c => c.trim()).filter(c => c.length > 0)

  const headers = parseRow(lines[0])
  const rows = lines.slice(2).map(parseRow)

  return (
    <div className="overflow-x-auto my-3">
      <table className="w-full text-sm border border-earth-200 rounded">
        <thead>
          <tr className="bg-earth-50">
            {headers.map((h, i) => (
              <th key={i} className="px-3 py-2 text-left text-xs font-semibold text-earth-700 border-b border-earth-200">
                {renderInline(h)}
              </th>
            ))}
          </tr>
        </thead>
        <tbody>
          {rows.map((row, i) => (
            <tr key={i} className="border-b border-earth-100">
              {row.map((cell, j) => (
                <td key={j} className="px-3 py-2 text-earth-600">
                  {renderInline(cell)}
                </td>
              ))}
            </tr>
          ))}
        </tbody>
      </table>
    </div>
  )
}
