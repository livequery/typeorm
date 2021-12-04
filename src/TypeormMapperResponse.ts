import { createParamDecorator, ExecutionContext } from "@nestjs/common"

export const TypeormMapperResponseKey = Symbol()


export const TypeormMapperResponse = createParamDecorator(
    (data: unknown, ctx: ExecutionContext) => {
        const request = ctx.switchToHttp().getRequest();
        return request[TypeormMapperResponseKey]
    },
);