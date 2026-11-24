// Обёртка fetch для admin-API того же origin. Ничего не сохраняется в браузере (ни cookie, ни storage, ни кэш).

export class ApiError extends Error {
    // problem - разобранный application/problem+json (или null), status - HTTP-статус (0 - сеть недоступна).
    constructor(status, problem, fallback) {
        super((problem && (problem.detail || problem.title)) || fallback || 'Ошибка запроса');
        this.status = status;
        this.problem = problem;
    }
}

async function request(method, path, body) {
    const init = { method, cache: 'no-store', credentials: 'omit', headers: { Accept: 'application/json, application/problem+json' } };
    if (body !== undefined) {
        init.headers['Content-Type'] = 'application/json';
        init.body = JSON.stringify(body);
    }

    let response;
    try {
        response = await fetch(path, init);
    } catch {
        throw new ApiError(0, null, 'Сервис недоступен');
    }

    let data = null;
    const type = response.headers.get('Content-Type') || '';
    if (type.includes('json')) {
        try {
            data = await response.json();
        } catch {
            data = null;
        }
    }

    if (!response.ok) {
        throw new ApiError(response.status, data, `HTTP ${response.status}`);
    }

    return data;
}

export const api = {
    get: path => request('GET', path),
    post: (path, body) => request('POST', path, body ?? {}),
};
