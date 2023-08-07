import { createDatasourceMapper } from "@livequery/nestjs";
import { RouteOptions } from "./RouteOptions.js";
import { TypeormDatasource } from "./TypeormDatasource.js";
import { DataSource, EntityTarget } from 'typeorm'
import { getDataSourceToken } from '@nestjs/typeorm'
import { getEntityName } from "./helpers/getEntityName.js";

export const [UseTypeormDatasource, getMetadatas] = createDatasourceMapper<RouteOptions>(TypeormDatasource)


export const getCollectionSchemaRefs = (connection_name: string = 'default') => getMetadatas()
    .map(option => ({
        ...option,
        connection_name: option.connection_name || 'default',
        collection_name: getEntityName(option.entity)
    }))
    .filter(x => x.connection_name == connection_name)
    .filter(o => o.realtime)
    .reduce((p, c) => {
        const found = p.get(c.collection_name);
        if (found && found.entity != c.entity) {
            console.error(`Duplicate collection name on different entity`);
            throw new Error();
        }
        const schema_refs = new Set([
            ...(found?.schema_refs.map(s => s.join('/')) || []),
            ...c.refs
        ])
        p.set(c.collection_name, {
            entity: c.entity,
            schema_refs: [...schema_refs].map(s => s.split('/'))
        });
        return p;
    }, new Map<string, {
        entity: EntityTarget<any>,
        schema_refs: string[][]
    }>())


export const TypeormDatasourceProviderWithMultipleConnections = (connection_names: string[] = []) => {

    return {
        provide: TypeormDatasource,
        inject: [undefined, ...connection_names].map(connection_name => getDataSourceToken(connection_name)),
        useFactory: (...connections: DataSource[]) => {

            const options_with_name = getMetadatas().map(option => ({
                ...option,
                collection_name: getEntityName(option.entity)
            }))

            return new TypeormDatasource(
                connections,
                options_with_name
            )
        }
    }
}


export const TypeormDatasourceProvider = TypeormDatasourceProviderWithMultipleConnections() 