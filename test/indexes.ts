import * as RDSDataService from 'aws-sdk/clients/rdsdataservice'
import { ExactFilter, OrdFilter, Query } from 'imes'

import {
  AuroraPostgresStore,
  AuroraPostgresExactIndex,
  AuroraPostgresOrdIndex,
  auroraLongValue,
  auroraNullable,
  auroraStringValue,
} from '../src'

jest.mock('aws-sdk/clients/rdsdataservice')

const commonQueryParams = {
  resourceArn: 'cluster-123',
  secretArn: 'secret-123',
  database: 'product',
}

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

export interface UserQuery extends Query<User> {
  filter?: {
    name?: ExactFilter<string>
    age?: OrdFilter<number>
  }
}

const store = new AuroraPostgresStore<User, UserQuery>({
  clusterArn: 'cluster-123',
  secretArn: 'secret-123',
  table: 'users',
  database: 'product',
  indexes: {
    name: new AuroraPostgresExactIndex(
      auroraStringValue,
      (user: User) => user.data.name
    ),
    age: new AuroraPostgresOrdIndex(
      auroraNullable(auroraLongValue),
      (user: User) => user.data.age
    ),
  },
})

const mockedRdsClient: jest.Mocked<RDSDataService> = store.client as jest.Mocked<
  RDSDataService
>

const user1: User = {
  data: {
    name: 'Trevor',
    age: 47,
  },
  meta: { createdAt: 'yesterday' },
  key: 'u1',
}

const user2: User = {
  data: {
    name: 'Whatever',
    age: 15,
  },
  meta: { createdAt: 'today' },
  key: 'u2',
}

const user3: User = {
  data: {
    name: 'Eternal',
    age: null,
  },
  meta: { createdAt: 'now' },
  key: 'u3',
}

test('AuroraPostgresStore#create', async () => {
  mockedRdsClient.executeStatement = jest.fn(() => ({
    promise: jest.fn().mockResolvedValue({ records: [] }),
  })) as any

  await store.create(user1)

  expect(mockedRdsClient.executeStatement).toHaveBeenCalledWith({
    ...commonQueryParams,
    sql: `
  INSERT INTO users (id, item, name, age)
  VALUES(:id, :item::jsonb, :name, :age)
`,
    parameters: [
      {
        name: 'id',
        value: {
          stringValue: 'u1',
        },
      },
      {
        name: 'item',
        value: {
          stringValue: JSON.stringify(user1),
        },
      },
      {
        name: 'name',
        value: {
          stringValue: 'Trevor',
        },
      },
      {
        name: 'age',
        value: {
          longValue: 47,
        },
      },
    ],
  })
})

test('AuroraPostgresStore#create with a null value', async () => {
  mockedRdsClient.executeStatement = jest.fn(() => ({
    promise: jest.fn().mockResolvedValue({ records: [] }),
  })) as any

  await store.create(user3)

  expect(mockedRdsClient.executeStatement).toHaveBeenCalledWith({
    ...commonQueryParams,
    sql: `
  INSERT INTO users (id, item, name, age)
  VALUES(:id, :item::jsonb, :name, :age)
`,
    parameters: [
      {
        name: 'id',
        value: {
          stringValue: 'u3',
        },
      },
      {
        name: 'item',
        value: {
          stringValue: JSON.stringify(user3),
        },
      },
      {
        name: 'name',
        value: {
          stringValue: 'Eternal',
        },
      },
      {
        name: 'age',
        value: {
          isNull: true,
        },
      },
    ],
  })
})

test('AuroraPostgresStore#update', async () => {
  mockedRdsClient.executeStatement = jest.fn(() => ({
    promise: jest.fn().mockResolvedValue({ records: [] }),
  })) as any

  await store.update(user1)

  expect(mockedRdsClient.executeStatement).toHaveBeenCalledWith({
    ...commonQueryParams,
    sql: `
  UPDATE users
  SET item = :item::jsonb, name = :name, age = :age
  WHERE id = :id
`,
    parameters: [
      {
        name: 'id',
        value: {
          stringValue: 'u1',
        },
      },
      {
        name: 'item',
        value: {
          stringValue: JSON.stringify(user1),
        },
      },
      {
        name: 'name',
        value: {
          stringValue: 'Trevor',
        },
      },
      {
        name: 'age',
        value: {
          longValue: 47,
        },
      },
    ],
  })
})

test('AuroraPostgresStore#get', async () => {
  const executeStatementResponse: RDSDataService.ExecuteStatementResponse = {
    records: [[{ stringValue: JSON.stringify(user1) }]],
  }

  mockedRdsClient.executeStatement = jest.fn(() => ({
    promise: jest.fn().mockResolvedValue(executeStatementResponse),
  })) as any

  expect(await store.get('u1')).toEqual(user1)

  expect(mockedRdsClient.executeStatement).toHaveBeenCalledWith({
    ...commonQueryParams,
    sql: `SELECT item FROM users WHERE id = :id`,
    parameters: [
      {
        name: 'id',
        value: {
          stringValue: 'u1',
        },
      },
    ],
  })

  mockedRdsClient.executeStatement = jest.fn(() => ({
    promise: jest.fn().mockResolvedValue({ records: [] }),
  })) as any

  expect(await store.get('dne')).toBeUndefined()
})

test('AuroraPostgresStore#find with eq filter', async () => {
  const executeStatementResponse: RDSDataService.ExecuteStatementResponse = {
    records: [[{ stringValue: 'u3' }, { stringValue: JSON.stringify(user3) }]],
  }

  mockedRdsClient.executeStatement = jest.fn(() => ({
    promise: jest.fn().mockResolvedValue(executeStatementResponse),
  })) as any

  expect(await store.find({ filter: { name: { eq: 'Eternal' } } })).toEqual({
    cursor: null,
    items: [user3],
  })

  expect(mockedRdsClient.executeStatement).toHaveBeenCalledWith({
    ...commonQueryParams,
    sql: `SELECT id,item FROM users WHERE name = :name__eq ORDER BY id`,
    parameters: [
      {
        name: 'name__eq',
        value: {
          stringValue: 'Eternal',
        },
      },
    ],
  })
})

test('AuroraPostgresStore#find with ne filter', async () => {
  const executeStatementResponse: RDSDataService.ExecuteStatementResponse = {
    records: [
      [{ stringValue: 'u1' }, { stringValue: JSON.stringify(user1) }],
      [{ stringValue: 'u2' }, { stringValue: JSON.stringify(user2) }],
    ],
  }

  mockedRdsClient.executeStatement = jest.fn(() => ({
    promise: jest.fn().mockResolvedValue(executeStatementResponse),
  })) as any

  expect(await store.find({ filter: { name: { ne: 'Eternal' } } })).toEqual({
    cursor: null,
    items: [user1, user2],
  })

  expect(mockedRdsClient.executeStatement).toHaveBeenCalledWith({
    ...commonQueryParams,
    sql: `SELECT id,item FROM users WHERE name <> :name__ne ORDER BY id`,
    parameters: [
      {
        name: 'name__ne',
        value: {
          stringValue: 'Eternal',
        },
      },
    ],
  })
})

test('AuroraPostgresStore#find with in filter', async () => {
  const executeStatementResponse: RDSDataService.ExecuteStatementResponse = {
    records: [[{ stringValue: 'u3' }, { stringValue: JSON.stringify(user3) }]],
  }

  mockedRdsClient.executeStatement = jest.fn(() => ({
    promise: jest.fn().mockResolvedValue(executeStatementResponse),
  })) as any

  expect(
    await store.find({ filter: { name: { in: ['Eternal', 'Sunshine'] } } })
  ).toEqual({
    cursor: null,
    items: [user3],
  })

  expect(mockedRdsClient.executeStatement).toHaveBeenCalledWith({
    ...commonQueryParams,
    sql: `SELECT id,item FROM users WHERE name IN (:name__in_0, :name__in_1) ORDER BY id`,
    parameters: [
      {
        name: 'name__in_0',
        value: {
          stringValue: 'Eternal',
        },
      },
      {
        name: 'name__in_1',
        value: {
          stringValue: 'Sunshine',
        },
      },
    ],
  })
})

test('AuroraPostgresStore#find with a gt filter', async () => {
  const executeStatementResponse: RDSDataService.ExecuteStatementResponse = {
    records: [[{ stringValue: 'u1' }, { stringValue: JSON.stringify(user1) }]],
  }

  mockedRdsClient.executeStatement = jest.fn(() => ({
    promise: jest.fn().mockResolvedValue(executeStatementResponse),
  })) as any

  expect(await store.find({ filter: { age: { gt: 45 } } })).toEqual({
    cursor: null,
    items: [user1],
  })

  expect(mockedRdsClient.executeStatement).toHaveBeenCalledWith({
    ...commonQueryParams,
    sql: `SELECT id,item FROM users WHERE age > :age_gt ORDER BY id`,
    parameters: [
      {
        name: 'age_gt',
        value: {
          longValue: 45,
        },
      },
    ],
  })
})
