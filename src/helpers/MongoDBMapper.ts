const { ObjectID } = require('mongodb')

export class MongoDBMapper {

    static toMongoDBObject<T extends { [key: string]: any }>(data: T) {
        if (!data) return data
        const { id, _id, ...rest } = data;
        return id ? { _id: ObjectID(id), ...rest } : data
    }

    static fromMongoDBObject<T extends { [key: string]: any }>(data: any) {
        if (!data) return data
        const { _id, id, ...rest } = data;
        return _id ? { id: _id, ...rest } : data
    }
}
