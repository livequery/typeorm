export const Cursor = {
    encode(v: any) {
        if (!v) return null
        return Buffer.from(JSON.stringify(v), 'utf8').toString('hex')
    },

    decode<T>(v: string) {
        if (!v) return {}
        try {
            return JSON.parse(Buffer.from(v, 'hex').toString('utf8')) as T
        } catch (e) {
            throw 'INVAILD_CURSOR'
        }
    }
} 