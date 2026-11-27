// Реестр клиентских модулей разделов. Новый раздел: файл модуля + одна строка здесь; каркас не меняется.
// Модуль: { id, title, render(container, api) }, id совпадает с id раздела из GET /admin/v1/sections.
// render может вернуть функцию очистки: она вызывается при уходе со страницы раздела (остановка опроса).

import reprocessing from './reprocessing.js';

export const sectionModules = [
    reprocessing,
];
