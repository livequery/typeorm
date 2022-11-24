import typeorm from 'typeorm'


export function getEntityName(entity) {
    return typeorm
        .getMetadataArgsStorage()
        .tables
        .find(t => t.target == entity).name
}