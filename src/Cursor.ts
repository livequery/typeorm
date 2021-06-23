export const Cursor = {
    encode(v: number) {
        return Buffer.from(JSON.stringify(v), 'utf8').toString('base64')
    },

    decode(v: string) {
        try{
            return JSON.parse(Buffer.from(v, 'base64').toString('utf8')) as number
        }catch(e){
            throw 'INVAILD_CURSOR'
        }
    }
}