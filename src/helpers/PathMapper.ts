export const PathMapper = (a: string | string[], b: string | string[]) => {
    return [a || '']
        .flat(2)
        .map(r1 => [b || ''].flat(2).map(r2 => `${r1}/${r2}`))
        .flat(2)
        .map(r => r.split('/')
        .filter(r => r != '')
        .join('/'))
}