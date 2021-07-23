export type PostgresDataChangePayload<T = any> = {
    id: string
    type: 'added' | 'modified' | 'removed'
    data: T
    refs: Array<{ ref: string, old_ref: string }>
    new_doc: T
}