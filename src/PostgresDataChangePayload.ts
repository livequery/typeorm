export type PostgresDataChangePayload<T> = {
    id: string
    type: 'added' | 'modified' | 'removed'
    data: T
    table: string
    old: any
    new: any
}