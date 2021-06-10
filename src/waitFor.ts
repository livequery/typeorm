export const waitFor = async<T>(fn: (...args: any[]) => Promise<T> | T, delay: number = 1000) => {
    while (true) {
        const rs = await fn()
        if (rs) return rs as T
        await await new Promise(s => setTimeout(s, delay))
    }
}