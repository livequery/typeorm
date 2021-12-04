export const JsonUtil = {

    deepJsonParse<T>(data: T) {

        const keys = Object.keys(data)

        const new_object = keys
            .filter(k => {
                const v = data[k] as string
                return typeof v == 'string' && v.startsWith('{"') && v.endsWith('}')
            })
            .reduce((p, c) => {
                try {
                    return { ...p, [c]: JSON.parse(data[c]) }
                } catch (e) {
                    return p
                }
            }, {})

        return { ...data, ...new_object } as T
    }
}