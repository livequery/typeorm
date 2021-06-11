export type PostgresDataChangePayload<T> = {
    id: string
    type: 'added' | 'modified' | 'removed'
    data: T
    refs: Array<{ ref: string, old_ref: string }>
    doc: T
}