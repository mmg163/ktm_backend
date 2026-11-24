// Раздел «Повторная обработка»: запуск задачи, список, прогресс, отчёт, отмена.
// Всё выводится через textContent; ничего не пишется в localStorage/sessionStorage/cookie.

const BASE = '/admin/v1/reprocessing/jobs';
const ACTIVE = new Set(['Queued', 'Running']);
const POLL_MS = 2000;
const REPORT_PAGE = 100;

function el(tag, props, ...children) {
    const e = document.createElement(tag);
    for (const [k, v] of Object.entries(props ?? {})) {
        if (k === 'class') {
            e.className = v;
        } else if (k === 'text') {
            e.textContent = v;
        } else if (k.startsWith('on')) {
            e.addEventListener(k.slice(2), v);
        } else {
            e.setAttribute(k, v);
        }
    }
    e.append(...children);
    return e;
}

function scalar(v) {
    if (v === null || v === undefined) {
        return '-';
    }
    return typeof v === 'object' ? JSON.stringify(v) : String(v);
}

function toUtc(localValue) {
    // datetime-local без пояса трактуется как UTC: "2026-01-01T10:00" -> "2026-01-01T10:00:00Z".
    if (!localValue) {
        return null;
    }
    return /T\d\d:\d\d$/.test(localValue) ? `${localValue}:00Z` : `${localValue.replace(/Z$/, '')}Z`;
}

function showProblem(box, e) {
    box.replaceChildren();
    box.hidden = false;
    box.classList.add('error');
    const p = e.problem;
    box.append(el('div', { text: p?.title && p.title !== e.message ? `${p.title}: ${e.message}` : e.message }));

    if (e.status === 400 && p?.errors) {
        const ul = el('ul');
        for (const [field, msgs] of Object.entries(p.errors)) {
            ul.append(el('li', { text: `${field}: ${[].concat(msgs).join('; ')}` }));
        }
        box.append(ul);
    }

    return p;
}

function table(headers, rows) {
    const thead = el('thead', {}, el('tr', {}, ...headers.map(h => el('th', { text: h }))));
    const tbody = el('tbody', {}, ...rows.map(r => el('tr', {}, ...r)));
    return el('table', {}, thead, tbody);
}

function cell(v) {
    return el('td', { text: scalar(v) });
}

function confirmDialog(title, pairs, okText, okClass) {
    return new Promise(resolve => {
        const dl = el('dl');
        for (const [k, v] of pairs) {
            dl.append(el('dt', { text: k }), el('dd', { text: v }));
        }

        const dialog = el('dialog', {});
        const ok = el('button', { type: 'button', class: okClass, text: okText });
        const cancel = el('button', { type: 'button', text: 'Назад' });
        dialog.append(el('h2', { text: title }), dl, el('menu', {}, cancel, ok));

        let answer = false;
        ok.addEventListener('click', () => { answer = true; dialog.close(); });
        cancel.addEventListener('click', () => dialog.close());
        dialog.addEventListener('close', () => { dialog.remove(); resolve(answer); });
        document.body.append(dialog);
        dialog.showModal();
    });
}

export default {
    id: 'reprocessing',
    title: 'Повторная обработка',

    render(container, api) {
        let timer = null;
        let selectedId = null;
        let disposed = false;

        const formError = el('div', { hidden: '' });
        const source = el('select', {}, ...['dlq', 'rejected', 'raw'].map(s => el('option', { value: s, text: s })));
        const from = el('input', { type: 'datetime-local', step: '1' });
        const to = el('input', { type: 'datetime-local', step: '1' });
        const reason = el('input', { type: 'text', maxlength: '200', autocomplete: 'off' });
        const maxRate = el('input', { type: 'number', min: '1', step: '1', autocomplete: 'off' });
        const submit = el('button', { type: 'submit', class: 'primary', text: 'Запустить' });

        const form = el('form', { novalidate: '' },
            el('label', {}, 'Источник', source),
            el('label', {}, el('span', { text: 'С (UTC)' }), from, el('span', { class: 'hint', text: 'для raw обязательно' })),
            el('label', {}, el('span', { text: 'По (UTC)' }), to),
            el('label', {}, el('span', { text: 'Причина (reason)' }), reason, el('span', { class: 'hint', text: 'не для raw' })),
            el('label', {}, el('span', { text: 'Скорость, соб/с' }), maxRate,
                el('span', { class: 'hint', text: 'пусто - по умолчанию сервиса; не больше предела MaxRateLimit' })),
            submit);

        const jobsBox = el('div');
        const jobBox = el('div');
        container.append(
            el('section', {}, el('h2', { text: 'Запуск' }), formError, form),
            el('section', {}, el('h2', { text: 'Задачи (последние 10)' }), jobsBox),
            el('section', {}, el('h2', { text: 'Задача' }), jobBox));

        function updateReasonState() {
            reason.disabled = source.value === 'raw';
            if (reason.disabled) {
                reason.value = '';
            }
        }

        source.addEventListener('change', updateReasonState);
        updateReasonState();

        function validate() {
            const errors = [];
            const f = toUtc(from.value);
            const t = toUtc(to.value);
            if (source.value === 'raw' && (!f || !t)) {
                errors.push('для raw нужно указать оба времени: с и по');
            } else if ((f && !t) || (!f && t)) {
                errors.push('укажите оба времени или ни одного');
            }
            if (f && t && Date.parse(f) >= Date.parse(t)) {
                errors.push('«С» должно быть раньше «По»');
            }
            if (maxRate.value !== '' && !(Number.isInteger(Number(maxRate.value)) && Number(maxRate.value) >= 1)) {
                errors.push('скорость - целое число не меньше 1');
            }
            return { errors, f, t };
        }

        form.addEventListener('submit', async ev => {
            ev.preventDefault();
            formError.hidden = true;
            const { errors, f, t } = validate();
            if (errors.length > 0) {
                showProblem(formError, Object.assign(new Error('Проверьте поля формы'), {
                    status: 400,
                    problem: { errors: { форма: errors } },
                }));
                return;
            }

            const body = { source: source.value };
            if (f) { body.from = f; }
            if (t) { body.to = t; }
            if (reason.value.trim() !== '') { body.reason = reason.value.trim(); }
            if (maxRate.value !== '') { body.maxRate = Number(maxRate.value); }

            const ok = await confirmDialog('Запустить повторную обработку?', [
                ['Источник', body.source],
                ['С (UTC)', body.from ?? 'не ограничено'],
                ['По (UTC)', body.to ?? 'не ограничено'],
                ['Причина', body.reason ?? '-'],
                ['Скорость, соб/с', body.maxRate !== undefined ? String(body.maxRate) : 'по умолчанию'],
            ], 'Запустить', 'primary');
            if (!ok || disposed) {
                return;
            }

            submit.disabled = true;
            try {
                const job = await api.post(BASE, body);
                await select(job.id);
                await refreshList();
            } catch (e) {
                const p = showProblem(formError, e);
                if (e.status === 409 && p?.activeJobId) {
                    formError.append(el('button', {
                        type: 'button', class: 'link', text: 'Открыть активную задачу',
                        onclick: () => select(p.activeJobId),
                    }));
                }
            } finally {
                submit.disabled = false;
            }
        });

        async function refreshList() {
            try {
                const data = await api.get(BASE);
                const items = Array.isArray(data) ? data : (data?.items ?? []);
                if (items.length === 0) {
                    jobsBox.textContent = 'Задач пока нет (состояние хранится в памяти сервиса).';
                    return;
                }
                jobsBox.replaceChildren(table(
                    ['Id', 'Источник', 'Состояние', 'Итог', 'Начало', ''],
                    items.map(j => [
                        cell(j.id), cell(j.parameters?.source ?? j.source), cell(j.state), cell(j.outcome), cell(j.startedAt),
                        el('td', {}, el('button', { type: 'button', class: 'link', text: 'Открыть', onclick: () => select(j.id) })),
                    ])));
            } catch (e) {
                showProblem(jobsBox, e);
            }
        }

        function stopPolling() {
            if (timer) {
                clearTimeout(timer);
                timer = null;
            }
        }

        async function select(id) {
            stopPolling();
            selectedId = id;
            await refreshJob();
        }

        async function refreshJob() {
            const id = selectedId;
            if (!id || disposed) {
                return;
            }

            let job;
            try {
                job = await api.get(`${BASE}/${encodeURIComponent(id)}`);
            } catch (e) {
                showProblem(jobBox, e);
                return;
            }

            if (disposed || id !== selectedId) {
                return;
            }

            renderJob(job);
            if (ACTIVE.has(job.state)) {
                timer = setTimeout(async () => { await refreshJob(); await refreshList(); }, POLL_MS);
            }
        }

        function renderJob(job) {
            jobBox.replaceChildren();
            jobBox.classList.remove('error');
            const active = ACTIVE.has(job.state);

            jobBox.append(el('div', { text: `Id: ${job.id}` }),
                el('div', { text: `Состояние: ${job.state}${job.outcome ? `, итог: ${job.outcome}` : ''}` }));

            const paramRows = Object.entries(job.parameters ?? {}).map(([k, v]) => [cell(k), cell(v)]);
            if (paramRows.length > 0) {
                jobBox.append(el('h3', { text: 'Параметры' }), table(['Параметр', 'Значение'], paramRows));
            }

            const counters = ['read', 'skippedByFilter', 'written', 'duplicates', 'rejectedAgain', 'failedAgain']
                .filter(k => job[k] !== undefined && job[k] !== null)
                .map(k => [cell(k), cell(job[k])]);
            if (counters.length > 0) {
                jobBox.append(el('h3', { text: 'Счётчики' }), table(['Счётчик', 'Значение'], counters));
            }

            const partitions = job.partitions ?? job.progress;
            if (Array.isArray(partitions) && partitions.length > 0) {
                const keys = Object.keys(partitions[0]);
                jobBox.append(el('h3', { text: 'Прогресс по партициям' }),
                    table(keys, partitions.map(p => keys.map(k => cell(p[k])))));
            }

            if (active) {
                jobBox.append(el('progress', { 'aria-label': 'Выполняется' }));
            }

            jobBox.append(el('div', { class: 'muted', text: `Начало: ${scalar(job.startedAt)}; завершение: ${scalar(job.finishedAt)}` }));
            if (active && job.estimatedRemainingSeconds !== null && job.estimatedRemainingSeconds !== undefined) {
                jobBox.append(el('div', { class: 'muted', text: `Осталось по оценке: ~${Math.ceil(job.estimatedRemainingSeconds)} с (сообщений в границах: ${scalar(job.totalMessages)})` }));
            }
            if (job.reportTruncated) {
                jobBox.append(el('div', { class: 'muted', text: 'Отчёт усечён: достигнут предел записей, счётчики полные.' }));
            }
            if (job.error) {
                jobBox.append(el('div', { class: 'error', text: `Ошибка: ${scalar(job.error.code ?? job.error)}` }));
            }

            if (active) {
                jobBox.append(el('button', { type: 'button', class: 'danger', text: 'Отменить', onclick: () => cancel(job) }));
            } else {
                const reportBox = el('div');
                jobBox.append(el('h3', { text: 'Отчёт' }), reportBox);
                loadReport(job.id, reportBox, 0);
            }
        }

        async function cancel(job) {
            const ok = await confirmDialog('Отменить задачу?', [
                ['Id', String(job.id)],
                ['Прогресс', `прочитано ${scalar(job.read)}, записано ${scalar(job.written)}`],
            ], 'Отменить задачу', 'danger');
            if (!ok || disposed) {
                return;
            }
            try {
                await api.post(`${BASE}/${encodeURIComponent(job.id)}/cancel`);
                await select(job.id);
            } catch (e) {
                const box = el('div');
                jobBox.append(box);
                showProblem(box, e);
            }
        }

        async function loadReport(id, box, offset) {
            try {
                const page = await api.get(`${BASE}/${encodeURIComponent(id)}/report?limit=${REPORT_PAGE}&offset=${offset}`);
                if (disposed || id !== selectedId) {
                    return;
                }
                const items = page?.items ?? [];
                if (offset === 0 && items.length === 0) {
                    box.textContent = 'В отчёте нет записей: все сообщения записаны или признаны дублями.';
                    return;
                }

                box.querySelectorAll('.pager').forEach(n => n.remove());
                let tbody = box.querySelector('tbody');
                if (!tbody) {
                    const keys = Object.keys(items[0]);
                    box.dataset.keys = keys.join(',');
                    box.append(table(keys, []));
                    tbody = box.querySelector('tbody');
                }

                const keys = box.dataset.keys.split(',');
                for (const it of items) {
                    tbody.append(el('tr', {}, ...keys.map(k => cell(it[k]))));
                }

                const next = offset + items.length;
                const total = page?.total ?? next;
                const pager = el('div', { class: 'pager muted', text: `Показано ${next} из ${total} ` });
                if (next < total && items.length > 0) {
                    pager.append(el('button', {
                        type: 'button', class: 'link', text: 'Показать ещё',
                        onclick: () => loadReport(id, box, next),
                    }));
                }
                box.append(pager);
            } catch (e) {
                showProblem(box, e);
            }
        }

        refreshList();

        return () => {
            disposed = true;
            stopPolling();
        };
    },
};
