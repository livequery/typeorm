import { getMetadataArgsStorage } from 'typeorm'


export function getEntityName(entity) {
    return getMetadataArgsStorage()
        .tables
        .find(t => t.target == entity).name
}