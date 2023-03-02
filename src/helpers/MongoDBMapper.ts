export class MongoDBMapper {

    static toMongoDBObject<T extends { [key: string]: any }>(data: T) {
        const { id, _id, ...rest } = data;
        return id ? { _id: id, ...rest } : data
    }

    static fromMongoDBObject<T extends { [key: string]: any }>(data: any) {
        const { _id, id, ...rest } = data;
        return _id ? { id: _id, ...rest } : data
    }
}
