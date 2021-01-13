import { DecoratorBuilder } from '@livequery/core'


export const [TypeormDatasource, listTypeormDatasources] = new DecoratorBuilder().createPropertyOrMethodDecorator<{
    entity: any,
    search_fields: string[]
}>()