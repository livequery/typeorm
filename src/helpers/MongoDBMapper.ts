export class MongoDBMapper {

    static toMongoDBObject<T extends { [key: string]: any }>(data: T) {
        const { id, ...rest } = data
        return { _id: id, ...rest } as any as T
    }

    static fromMongoDBObject<T extends { [key: string]: any }>(data: any) {
        const { _id, ...rest } = data
        return { id: _id, ...rest } as T
    }
}