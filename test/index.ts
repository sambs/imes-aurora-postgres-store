import test from 'tape'
import { OrdFilter, EqualFilter, PrefixFilter, Query } from 'imes'
import { AuroraPostgresStore } from '../src'

const store = new AuroraPostgresStore<User, UserQuery>({
  clusterArn: process.env.CLUSTER_ARN!,
  secretArn: process.env.SECRET_ARN!,
  table: 'events',
  database: 'tutorial',
})

interface UserData {
  name: string
  age: number | null
}

type UserKey = string

interface UserMeta {
  createdAt: string
}

interface User {
  data: UserData
  meta: UserMeta
  key: UserKey
}

interface UserQuery extends Query<User> {
  filter?: {
    name?: EqualFilter<string> & PrefixFilter
    age?: OrdFilter<number>
  }
}

const user: User = {
  data: {
    name: 'Trevor',
    age: 47,
  },
  meta: { createdAt: 'yesterday' },
  key: 'u1',
}

test('AuroraStore read and write', async t => {
  await store.write(user)

  t.pass('writes without erroring')

  t.equal(
    await store.read('dne'),
    undefined,
    'returns undefined when asked for a non-existant key'
  )

  t.deepEqual(await store.read('u1'), user, 'returns a stored item')

  t.end()
})
