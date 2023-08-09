import { ObjectId } from "typeorm";

export class MongoDBMapper {

    static toMongoDBObject<T extends { [key: string]: any }>(data: T) {
        if (!data) return data
        const { id, _id, ...rest } = data;
        return id ? { _id: new ObjectId(id), ...rest } : data
    }

    static toMongoDBQuery<T extends { [key: string]: any }>(data: T) {
        if (!data || !data.id) return data
        const { id, _id, ...rest } = data;
        if (typeof id == 'string') return { _id: new ObjectId(id), ...rest }
        if (typeof id == 'object') return {
            _id: Object.entries(id).reduce((p, [key, value]) => ({
                ...p,
                [key]: Array.isArray(value) ? value.map(v => new ObjectId(v)) : (
                    typeof value == 'string' ? new ObjectId(value) : value
                )
            }), {}),
            ...rest
        }
        return { _id: id, ...rest }
    }

    static fromMongoDBObject(data: any) {
        if (!data) return data
        const { _id, id, ...rest } = data;
        return _id ? { id: _id, ...rest } : data
    }
}
