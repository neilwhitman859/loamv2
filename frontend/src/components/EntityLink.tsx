import { Link } from 'react-router-dom'

interface Props {
  type: 'wines' | 'producers' | 'grapes' | 'appellations' | 'regions' | 'countries'
  id: string
  name: string
  className?: string
}

export default function EntityLink({ type, id, name, className }: Props) {
  return (
    <Link
      to={`/data/${type}/${id}`}
      className={className ?? 'text-wine-600 hover:text-wine-800 hover:underline'}
    >
      {name}
    </Link>
  )
}
