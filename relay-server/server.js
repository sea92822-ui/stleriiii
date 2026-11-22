/* Сервер для игры через интернет: запустите на компьютере с белым IP или на VPS.
     node server.js            — порт 27500
     node server.js 30000      — свой порт
   Откройте порт (TCP) в брандмауэре / пробросьте на роутере. В игре: «Мультиплеер» →
   адрес сервера (IP или домен:порт) и одинаковое название комнаты у всех игроков. */
'use strict';
const relay = require('./relay');
const port = +process.argv[2] || +process.env.PORT || 27500;
relay.start(port, m => console.log(new Date().toLocaleTimeString(), m)).then(r => {
  console.log('Сервер armorgame слушает порт ' + r.port);
  console.log('Адреса в локальной сети: ' + (r.ips.join(', ') || 'нет'));
}).catch(e => { console.error('Не удалось открыть порт ' + port + ': ' + e.message); process.exit(1); });
