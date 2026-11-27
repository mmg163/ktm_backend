// Каркас админки: меню по GET /admin/v1/sections, модули из реестра sections/index.js. Вывод только через textContent.

import { api } from './api.js';
import { sectionModules } from './sections/index.js';

const menu = document.getElementById('menu');
const content = document.getElementById('content');
const fatal = document.getElementById('fatal');

let cleanup = null;

async function open(module, button) {
    if (cleanup) {
        cleanup();
        cleanup = null;
    }

    for (const b of menu.children) {
        b.setAttribute('aria-current', b === button ? 'true' : 'false');
    }

    content.replaceChildren();
    const result = await module.render(content, api);
    cleanup = typeof result === 'function' ? result : null;
}

async function main() {
    let sections;
    try {
        sections = await api.get('/admin/v1/sections');
    } catch (e) {
        fatal.textContent = `Не удалось получить список разделов: ${e.message}`;
        fatal.hidden = false;
        return;
    }

    // Ответ - массив или объект с массивом; раздел без клиентского модуля скрыт.
    const list = Array.isArray(sections) ? sections : (sections?.sections ?? []);
    const byId = new Map(sectionModules.map(m => [m.id, m]));
    let first = null;

    for (const s of list) {
        const module = byId.get(s.id);
        if (!module) {
            continue;
        }

        const button = document.createElement('button');
        button.type = 'button';
        button.textContent = module.title || s.title || s.id;
        button.addEventListener('click', () => open(module, button));
        menu.append(button);
        first ??= { module, button };
    }

    if (first) {
        await open(first.module, first.button);
    } else {
        content.textContent = 'Нет доступных разделов.';
    }
}

window.addEventListener('pagehide', () => cleanup?.());
main();
