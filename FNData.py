import contextlib
import csv
import logging
import os.path
import queue
import time
from collections import deque
from datetime import datetime, timezone, timedelta, time as datetime_time
from threading import Thread, Event  # Поток и событие остановки потока получения новых бар по расписанию биржи
from typing import Generator
from uuid import uuid4  # Номера расписаний должны быть уникальными во времени и пространстве

from backtrader import TimeFrame, date2num
from backtrader.feed import AbstractDataBase
from backtrader.utils.py3 import with_metaclass
from google.protobuf.json_format import MessageToDict
from google.protobuf.timestamp_pb2 import Timestamp

from BackTraderFinam import FNStore
from FinamPy.proto.candles_pb2 import (
    DayCandleTimeFrame,
    DayCandleInterval,
    IntradayCandleTimeFrame,
    IntradayCandleInterval,
)
from FinamPy.proto.google.type.date_pb2 import Date


class MetaFNData(AbstractDataBase.__class__):
    def __init__(self, name, bases, dct):
        super(MetaFNData, self).__init__(name, bases, dct)  # Инициализируем класс данных
        FNStore.DataCls = self  # Регистрируем класс данных в хранилище Финам


class _FileField:
    TICKER = '<TICKER>'
    DATE = '<DATE>'
    TIME = '<TIME>'
    OPEN = '<OPEN>'
    HIGH = '<HIGH>'
    LOW = '<LOW>'
    CLOSE = '<CLOSE>'
    VOL = '<VOL>'

    ALL = (TICKER, DATE, TIME, OPEN, HIGH, LOW, CLOSE, VOL)


class FNData(with_metaclass(MetaFNData, AbstractDataBase)):
    """Данные Финам"""
    params = (
        ('account_id', 0),  # Порядковый номер счета
        ('four_price_doji', False),  # False - не пропускать дожи 4-х цен, True - пропускать
        ('schedule', None),  # Расписание работы биржи
        ('live_bars', False),  # False - только история, True - история и новые бары
    )
    datapath = os.path.join(os.path.dirname(os.path.realpath(__file__)), '..', 'Data', 'Finam', '')  # Путь сохранения файла истории
    delimiter = '\t'  # Разделитель значений в файле истории. По умолчанию табуляция
    dt_format = '%d.%m.%Y %H:%M'  # Формат представления даты и времени в файле истории. По умолчанию русский формат
    sleep_time_sec = 1  # Время ожидания в секундах, если не пришел новый бар. Для снижения нагрузки/энергопотребления процессора

    def islive(self):
        """Если подаем новые бары, то Cerebro не будет запускать preload и runonce, т.к. новые бары должны идти один за другим"""
        return self.p.live_bars

    def __init__(self):
        self.store = FNStore()  # Хранилище Финам
        self.intraday = self.p.timeframe == TimeFrame.Minutes  # Внутридневной временной интервал
        self.board, self.symbol = self.store.provider.dataname_to_board_symbol(self.p.dataname)  # По тикеру получаем код режима торгов и тикера
        self.client_id = self.store.provider.client_ids[self.p.account_id]  # Счет тикера
        self.finam_timeframe = self.bt_timeframe_to_finam_timeframe(self.p.timeframe, self.p.compression)  # Конвертируем временной интервал из BackTrader в Финам
        self.tf = self.bt_timeframe_to_tf(self.p.timeframe, self.p.compression)  # Конвертируем временной интервал из BackTrader для имени файла истории и расписания
        self.file = f'{self.board}.{self.symbol}_{self.tf}'  # Имя файла истории
        self.logger = logging.getLogger(f'FNData.{self.file}')  # Будем вести лог
        self.file_name = f'{self.datapath}{self.file}.txt'  # Полное имя файла истории
        self.history_bars = deque()  # Исторические бары из файла и истории после проверки на соответствие условиям выборки
        self.guid = None  # Идентификатор подписки/расписания на историю цен
        self.exit_event = Event()  # Определяем событие выхода из потока
        self.dt_last_open = datetime.min  # Дата и время открытия последнего полученного бара
        self.last_bar_received = False  # Получен последний бар
        self.live_mode = False  # Режим получения бар. False = История, True = Новые бары

    def setenvironment(self, env):
        """Добавление хранилища Финам в cerebro"""
        super(FNData, self).setenvironment(env)
        env.addstore(self.store)  # Добавление хранилища Финам в cerebro

    def start(self):
        super(FNData, self).start()
        self.put_notification(self.DELAYED)  # Отправляем уведомление об отправке исторических (не новых) бар
        self.get_bars_from_file()  # Получаем бары из файла
        self.get_bars_from_history()  # Получаем бары из истории
        if len(self.history_bars) > 0:  # Если был получен хотя бы 1 бар
            self.put_notification(self.CONNECTED)  # то отправляем уведомление о подключении и начале получения исторических бар
        if self.p.live_bars:  # Если получаем историю и новые бары
            if self.p.schedule:  # Если получаем новые бары по расписанию
                self.guid = str(uuid4())  # guid расписания
                Thread(target=self.stream_bars).start()  # Создаем и запускаем получение новых бар по расписанию в потоке
            else:  # Если получаем новые бары по подписке
                raise NotImplementedError  # TODO Ждем от Финама подписку на бары

    def _load(self):
        """Загрузка бара из истории или нового бара"""
        if len(self.history_bars) > 0:  # Если есть исторические данные
            bar = self.history_bars.popleft()  # Берем и удаляем первый бар из хранилища. С ним будем работать

        elif not self.p.live_bars:  # Если получаем только историю (self.history_bars) и исторических данных нет / все исторические данные получены
            self.put_notification(self.DISCONNECTED)  # Отправляем уведомление об окончании получения исторических бар
            self.logger.debug('Бары из файла/истории отправлены в ТС. Новые бары получать не нужно. Выход')
            return False  # Больше сюда заходить не будем

        else:  # Если получаем историю и новые бары (self.store.new_bars)
            new_bars = self.store.new_bars[self.guid]  # Смотрим в хранилище новых бар бары с guid подписки
            try:
                new_bar = new_bars.get(timeout=1)
            except queue.Empty:
                return None

            # self.last_bar_received = new_bars.qsize() == 1  # Если в хранилище остался 1 бар, то мы будем получать последний возможный бар
            self.last_bar_received = new_bars.qsize() == 0  # Если в хранилище не осталось баров, значит это последний бар
            if self.last_bar_received:  # Получаем последний возможный бар
                self.logger.debug('Получение последнего возможного на данный момент бара')

            # Бар из хранилища новых бар
            bar = {
                'datetime': (self.get_bar_open_date_time(new_bar)),
                'open': self.store.provider.dict_decimal_to_float(new_bar['open']),
                'high': self.store.provider.dict_decimal_to_float(new_bar['high']),
                'low': self.store.provider.dict_decimal_to_float(new_bar['low']),
                'close': self.store.provider.dict_decimal_to_float(new_bar['close']),
                'volume': new_bar['volume'],
            }
            if not self.is_bar_valid(bar):  # Если бар не соответствует всем условиям выборки
                return None  # то пропускаем бар, будем заходить еще
            self.logger.debug(f'Сохранение нового бара с {bar["datetime"].strftime(self.dt_format)} в файл')
            self.save_bar_to_file(bar)  # Сохраняем бар в конец файла
            if self.last_bar_received and not self.live_mode:  # Если получили последний бар и еще не находимся в режиме получения новых бар (LIVE)
                self.put_notification(self.LIVE)  # Отправляем уведомление о получении новых бар
                self.live_mode = True  # Переходим в режим получения новых бар (LIVE)
            elif self.live_mode and not self.last_bar_received:  # Если находимся в режиме получения новых бар (LIVE)
                self.put_notification(self.DELAYED)  # Отправляем уведомление об отправке исторических (не новых) бар
                self.live_mode = False  # Переходим в режим получения истории

        # Все проверки пройдены. Записываем полученный исторический/новый бар
        self.lines.datetime[0] = date2num(bar['datetime'])  # DateTime
        self.lines.open[0] = self.store.provider.finam_price_to_price(self.board, self.symbol, bar['open'])  # Open
        self.lines.high[0] = self.store.provider.finam_price_to_price(self.board, self.symbol, bar['high'])  # High
        self.lines.low[0] = self.store.provider.finam_price_to_price(self.board, self.symbol, bar['low'])  # Low
        self.lines.close[0] = self.store.provider.finam_price_to_price(self.board, self.symbol, bar['close'])  # Close
        self.lines.volume[0] = int(bar['volume'])  # Volume подается как строка. Его обязательно нужно привести к целому
        self.lines.openinterest[0] = 0  # Открытый интерес в Финам не учитывается
        return True  # Будем заходить сюда еще

    def stop(self):
        super(FNData, self).stop()
        self.logger.info('Stopping')
        if self.p.live_bars:  # Если была подписка/расписание
            if self.p.schedule:  # Если получаем новые бары по расписанию
                self.exit_event.set()  # то отменяем расписание
            else:  # Если получаем новые бары по подписке
                raise NotImplementedError  # TODO Ждем от Финама подписку на бары
            self.put_notification(self.DISCONNECTED)  # Отправляем уведомление об окончании получения новых бар
        self.store.DataCls = None  # Удаляем класс данных в хранилище

    # Получение бар

    def get_bars_from_file(self) -> None:
        """Получение бар из файла"""
        if not os.path.isfile(self.file_name):  # Если файл не существует
            return  # то выходим, дальше не продолжаем
        self.logger.debug(f'Получение бар из файла {self.file_name}')
        with open(self.file_name) as file:  # Открываем файл на последовательное чтение
            reader = csv.DictReader(file, fieldnames=_FileField.ALL)  # Данные в строке разделены табуляцией
            next(reader, None)  # Пропускаем первую строку с заголовками
            for row in reader:  # type: dict
                bar = _bar_from_csv_row(row)
                if self.is_bar_valid(bar):  # Если исторический бар соответствует всем условиям выборки
                    self.history_bars.append(bar)  # то добавляем бар
        if len(self.history_bars) > 0:  # Если были получены бары из файла
            self.logger.debug(f'Получено бар из файла: {len(self.history_bars)} с {self.history_bars[0]["datetime"].strftime(self.dt_format)} по {self.history_bars[-1]["datetime"].strftime(self.dt_format)}')
        else:  # Бары из файла не получены
            self.logger.debug('Из файла новых бар не получено')

    def get_bars_from_history(self) -> None:
        """Получение бар из истории группами по кол-ву бар и дней"""
        file_history_bars_len = len(self.history_bars)  # Кол-во полученных бар из файла для лога
        if self.dt_last_open > datetime.min:  # Если в файле были бары
            last_date = self.dt_last_open  # Дата и время последнего бара из файла по МСК
            next_bar_open_utc = self.store.provider.msk_to_utc_datetime(last_date + timedelta(minutes=1), True) if self.intraday else \
                last_date.replace(tzinfo=timezone.utc) + timedelta(days=1)  # Смещаем время на возможный следующий бар по UTC
        else:  # Если в файле не было баров
            next_bar_open_utc = datetime(1990, 1, 1, tzinfo=timezone.utc)  # то берем дату, когда никакой тикер еще не торговался
        interval = IntradayCandleInterval(count=500) if self.intraday else DayCandleInterval(count=500)  # Максимальное кол-во бар для истории 500
        td = timedelta(days=(30 if self.intraday else 365))  # Максимальный запрос за 30 дней для внутридневных интервалов и 1 год (365 дней) для дневных и выше
        todate_utc = datetime.utcnow().replace(tzinfo=timezone.utc)  # Будем получать бары до текущей даты и времени UTC
        from_ = getattr(interval, 'from')  # Т.к. from - ключевое слово в Python, то получаем атрибут from из атрибута интервала
        to_ = getattr(interval, 'to')  # Аналогично будем работать с атрибутом to для единообразия
        first_request = not os.path.isfile(self.file_name)  # Если файл не существует, то первый запрос будем формировать без даты окончания. Так мы в первом запросе получим первые бары истории
        with self._with_history_bars_file_writer() as writer:
            while True:  # Будем получать бары пока не получим все
                todate_min_utc = min(todate_utc, next_bar_open_utc + td)  # До какой даты можем делать запрос
                if self.intraday:  # Для интрадея datetime -> Timestamp
                    from_.seconds = Timestamp(seconds=int(next_bar_open_utc.timestamp())).seconds  # Дата и время начала интервала UTC
                    if not first_request:  # Для всех запросов, кроме первого
                        to_.seconds = Timestamp(seconds=int(todate_min_utc.timestamp())).seconds  # Дата и время окончания интервала UTC
                        if from_.seconds == to_.seconds:  # Если дата и время окончания интервала совпадает с датой и временем начала
                            break  # то выходим из цикла получения бар
                else:  # Для дневных интервалов и выше datetime -> Date
                    date_from = Date(year=next_bar_open_utc.year, month=next_bar_open_utc.month, day=next_bar_open_utc.day)  # Дата начала интервала UTC
                    from_.year = date_from.year
                    from_.month = date_from.month
                    from_.day = date_from.day
                    if not first_request:  # Для всех запросов, кроме первого
                        date_to = Date(year=todate_min_utc.year, month=todate_min_utc.month, day=todate_min_utc.day)  # Дата окончания интервала UTC
                        if date_to == date_from:  # Если дата окончания интервала совпадает с датой начала
                            break  # то выходим из цикла получения бар
                        to_.year = date_to.year
                        to_.month = date_to.month
                        to_.day = date_to.day
                if first_request:  # Для первого запроса
                    first_request = False  # далее будем ставить в запросы дату окончания интервала
                self.logger.debug(f'Получение бар из истории с {next_bar_open_utc} по {todate_min_utc}')
                response = (self.store.provider.get_intraday_candles(self.board, self.symbol, self.finam_timeframe, interval) if self.intraday else
                            self.store.provider.get_day_candles(self.board, self.symbol, self.finam_timeframe, interval))  # Получаем ответ на запрос бар
                if not response:  # Если в ответ ничего не получили
                    self.logger.warning('Ошибка запроса бар из истории')
                    return  # то выходим, дальше не продолжаем
                else:  # Если в ответ пришли бары
                    response_dict = MessageToDict(response, always_print_fields_with_no_presence=True)  # Переводим в словарь из JSON
                    if 'candles' not in response_dict:  # Если бар нет в словаре
                        self.logger.error(f'Бар (candles) нет в словаре {response_dict}')
                        return  # то выходим, дальше не продолжаем
                new_bars_dict = response_dict['candles']  # Получаем все бары из Finam
                if len(new_bars_dict) > 0:  # Если пришли новые бары
                    first_bar_open_dt = self.get_bar_open_date_time(new_bars_dict[0])  # Дату и время первого полученного бара переводим из UTC в МСК
                    last_bar_open_dt = self.get_bar_open_date_time(new_bars_dict[-1])  # Дату и время последнего полученного бара переводим из UTC в МСК
                    self.logger.debug(f'Получены бары с {first_bar_open_dt} по {last_bar_open_dt}')
                    t = time.perf_counter()
                    for new_bar in new_bars_dict:  # Пробегаемся по всем полученным барам
                        bar = {
                            'datetime': self.get_bar_open_date_time(new_bar),
                            'open': self.store.provider.dict_decimal_to_float(new_bar['open']),
                            'high': self.store.provider.dict_decimal_to_float(new_bar['high']),
                            'low': self.store.provider.dict_decimal_to_float(new_bar['low']),
                            'close': self.store.provider.dict_decimal_to_float(new_bar['close']),
                            'volume': new_bar['volume'],
                        }
                        self.save_bar_to_file(bar, writer=writer)  # Сохраняем бар в файл
                        if self.is_bar_valid(bar):  # Если исторический бар соответствует всем условиям выборки
                            self.history_bars.append(bar)  # то добавляем бар

                    self.logger.debug(f'В файл {self.file_name} записано {len(new_bars_dict)} баров на {new_bars_dict[-1]["timestamp"]} {time.perf_counter() - t:.3f}s')

                    last_bar_open_utc = self.store.provider.msk_to_utc_datetime(last_bar_open_dt, True) if self.intraday else \
                        last_bar_open_dt.replace(tzinfo=timezone.utc)  # Дата и время открытия последнего бара UTC
                    next_bar_open_utc = last_bar_open_utc + timedelta(minutes=1) if self.intraday else last_bar_open_utc + timedelta(days=1)  # Смещаем время на возможный следующий бар UTC
                else:  # Если новых бар нет
                    next_bar_open_utc = todate_min_utc + timedelta(minutes=1) if self.intraday else todate_min_utc + timedelta(days=1)  # то смещаем время на возможный следующий бар UTC
                if next_bar_open_utc > todate_utc:  # Если пройден весь интервал
                    break  # то выходим из цикла получения бар

        if len(self.history_bars) - file_history_bars_len > 0:  # Если получены бары из истории
            self.logger.debug(f'Получено бар из истории: {len(self.history_bars) - file_history_bars_len} с {self.history_bars[file_history_bars_len]["datetime"].strftime(self.dt_format)} по {self.history_bars[-1]["datetime"].strftime(self.dt_format)}')
        else:  # Бары из истории не получены
            self.logger.debug('Из истории новых бар не получено')

    def stream_bars(self) -> None:
        """Поток получения новых бар по расписанию биржи"""
        self.logger.debug('Запуск получения новых бар по расписанию')
        interval = IntradayCandleInterval(count=10) if self.intraday else DayCandleInterval(count=1)  # Принимаем последний завершенный бар
        from_ = getattr(interval, 'from')  # т.к. from - ключевое слово в Python, то получаем атрибут from из атрибута интервала
        while True:
            market_datetime_now = self.p.schedule.utc_to_msk_datetime(datetime.utcnow())  # Текущее время на бирже
            trade_bar_open_datetime = self.p.schedule.trade_bar_open_datetime(market_datetime_now, self.tf)  # Дата и время открытия бара, который будем получать
            trade_bar_request_datetime = self.p.schedule.trade_bar_request_datetime(market_datetime_now, self.tf)  # Дата и время запроса бара на бирже
            # FIXME: убираем тупую задержку
            trade_bar_request_datetime = trade_bar_request_datetime - self.p.schedule.delta
            sleep_time_secs = (trade_bar_request_datetime - market_datetime_now).total_seconds()  # Время ожидания в секундах
            self.logger.debug(f'Получение новых бар с {trade_bar_open_datetime.strftime(self.dt_format)} по расписанию в {trade_bar_request_datetime.strftime(self.dt_format)}. Ожидание {sleep_time_secs} с')

            exit_event_set = self.exit_event.wait(sleep_time_secs)  # Ждем нового бара или события выхода из потока
            if exit_event_set:  # Если произошло событие выхода из потока
                self.logger.warning('Отмена получения новых бар по расписанию')
                return  # то выходим из потока, дальше не продолжаем

            if self.intraday:  # Для интрадея datetime -> Timestamp
                seconds_from = self.p.schedule.msk_datetime_to_utc_timestamp(trade_bar_open_datetime)  # Дата и время бара в timestamp UTC
                date_from = Timestamp(seconds=seconds_from)  # Дата и время начала интервала UTC
                from_.seconds = date_from.seconds

            else:  # Для дневных интервалов и выше datetime -> Date
                trade_bar_open_dt_utc = trade_bar_open_datetime  # Дата бара передается с нулевым временем. Поэтому, конвертировать в UTC не нужно
                date_from = Date(year=trade_bar_open_dt_utc.year, month=trade_bar_open_dt_utc.month, day=trade_bar_open_dt_utc.day)  # Дата начала интервала UTC
                from_.year = date_from.year
                from_.month = date_from.month
                from_.day = date_from.day

            # noinspection PyBroadException
            try:
                # Получаем внутридневные или дневные бары
                resp = self.request_closed_bar(interval)
            except Exception:
                self.logger.exception('error requesting closed bar')
                continue

            if not resp:  # Если в ответ ничего не получили
                self.logger.warning('Ошибка запроса бар из истории по расписанию')
                continue  # то будем получать следующий бар

            bars = resp['candles']  # Последний сформированный и текущий несформированный (если имеется) бары
            if len(bars) == 0:  # Если бары не получены
                self.logger.warning('Новые бары по расписанию не получены')
                continue  # то будем получать следующий бар

            bar = bars[0]  # Получаем первый (завершенный) бар
            self.logger.debug(f'Получен бар: {self.board}.{self.symbol}: {bar}')

            elapsed = self.p.schedule.utc_to_msk_datetime(datetime.utcnow()) - trade_bar_request_datetime
            self.logger.info(f'Задержка получения бара: {elapsed.total_seconds() * 1000:.3f}ms')

            self.store.new_bars[self.guid].put(bar)  # Добавляем в хранилище новых бар

    # Функции

    @staticmethod
    def bt_timeframe_to_finam_timeframe(timeframe, compression=1):
        """Перевод временнОго интервала из BackTrader в Финам

        :param TimeFrame timeframe: Временной интервал
        :param int compression: Размер временнОго интервала
        :return: Временной интервал Финам
        """
        if timeframe == TimeFrame.Days:  # Дневной временной интервал (по умолчанию)
            return DayCandleTimeFrame.DAYCANDLE_TIMEFRAME_D1
        elif timeframe == TimeFrame.Weeks:  # Недельный временной интервал
            return DayCandleTimeFrame.DAYCANDLE_TIMEFRAME_W1
        elif timeframe == TimeFrame.Minutes:  # Минутный временной интервал
            if compression == 60:  # Часовой временной интервал
                return IntradayCandleTimeFrame.INTRADAYCANDLE_TIMEFRAME_H1
            elif compression == 15:  # 15-и минутный временной интервал
                return IntradayCandleTimeFrame.INTRADAYCANDLE_TIMEFRAME_M15
            elif compression == 5:  # 5-и минутный временной интервал
                return IntradayCandleTimeFrame.INTRADAYCANDLE_TIMEFRAME_M5
            elif compression == 1:  # 1 минутный временной интервал
                return IntradayCandleTimeFrame.INTRADAYCANDLE_TIMEFRAME_M1
        else:  # В остальных случаях
            return DayCandleTimeFrame.DAYCANDLE_TIMEFRAME_D1  # возвращаем значение по умолчанию

    @staticmethod
    def bt_timeframe_to_tf(timeframe, compression=1) -> str:
        """Перевод временнОго интервала из BackTrader для имени файла истории и расписания https://ru.wikipedia.org/wiki/Таймфрейм

        :param TimeFrame timeframe: Временной интервал
        :param int compression: Размер временнОго интервала
        :return: Временной интервал для имени файла истории и расписания
        """
        if timeframe == TimeFrame.Minutes:  # Минутный временной интервал
            return f'M{compression}'
        # Часовой график f'H{compression}' заменяем минутным. Пример: H1 = M60
        elif timeframe == TimeFrame.Days:  # Дневной временной интервал
            return f'D1'
        elif timeframe == TimeFrame.Weeks:  # Недельный временной интервал
            return f'W1'
        elif timeframe == TimeFrame.Months:  # Месячный временной интервал
            return f'MN1'
        elif timeframe == TimeFrame.Years:  # Годовой временной интервал
            return f'Y1'
        raise NotImplementedError  # С остальными временнЫми интервалами не работаем

    def get_bar_open_date_time(self, bar) -> datetime:
        """Дата и время открытия бара. Переводим из GMT в MSK для интрадея. Оставляем в GMT для дневок и выше."""
        # Дату/время UTC получаем в формате ISO 8601. Пример: 2023-06-16T20:01:00Z
        # В статье https://stackoverflow.com/questions/127803/how-do-i-parse-an-iso-8601-formatted-date описывается проблема, что Z на конце нужно убирать
        return self.store.provider.utc_to_msk_datetime(
            datetime.fromisoformat(bar['timestamp'][:-1])) if self.intraday else \
            datetime(bar['date']['year'], bar['date']['month'], bar['date']['day'])  # Дату/время переводим из UTC в МСК

    def get_bar_close_date_time(self, dt_open, period=1) -> datetime:
        """Дата и время закрытия бара"""
        if self.p.timeframe == TimeFrame.Days:  # Дневной временной интервал (по умолчанию)
            return dt_open + timedelta(days=period)  # Время закрытия бара
        elif self.p.timeframe == TimeFrame.Weeks:  # Недельный временной интервал
            return dt_open + timedelta(weeks=period)  # Время закрытия бара
        elif self.p.timeframe == TimeFrame.Months:  # Месячный временной интервал
            year = dt_open.year + (dt_open.month + period - 1) // 12  # Год
            month = (dt_open.month + period - 1) % 12 + 1  # Месяц
            return datetime(year, month, 1)  # Время закрытия бара
        elif self.p.timeframe == TimeFrame.Years:  # Годовой временной интервал
            return dt_open.replace(year=dt_open.year + period)  # Время закрытия бара
        elif self.p.timeframe == TimeFrame.Minutes:  # Минутный временной интервал
            return dt_open + timedelta(minutes=self.p.compression * period)  # Время закрытия бара
        elif self.p.timeframe == TimeFrame.Seconds:  # Секундный временной интервал
            return dt_open + timedelta(seconds=self.p.compression * period)  # Время закрытия бара

    def is_bar_valid(self, bar) -> bool:
        """Проверка бара на соответствие условиям выборки"""
        dt_open = bar['datetime']  # Дата и время открытия бара МСК
        if dt_open <= self.dt_last_open:  # Если пришел бар из прошлого (дата открытия меньше последней даты открытия)
            self.logger.warning(f'Дата/время открытия бара {dt_open} <= последней даты/времени открытия {self.dt_last_open}')
            return False  # то бар не соответствует условиям выборки
        if self.p.fromdate and dt_open < self.p.fromdate or self.p.todate and dt_open > self.p.todate:  # Если задан диапазон, а бар за его границами
            # self.logger.debug(f'Дата/время открытия бара {dt_open} за границами диапазона {self.p.fromdate} - {self.p.todate}')
            self.dt_last_open = dt_open  # Запоминаем дату/время открытия пришедшего бара для будущих сравнений
            return False  # то бар не соответствует условиям выборки
        if self.p.sessionstart != datetime_time.min and dt_open.time() < self.p.sessionstart:  # Если задано время начала сессии и открытие бара до этого времени
            self.logger.warning(f'Дата/время открытия бара {dt_open} до начала торговой сессии {self.p.sessionstart}')
            self.dt_last_open = dt_open  # Запоминаем дату/время открытия пришедшего бара для будущих сравнений
            return False  # то бар не соответствует условиям выборки
        dt_close = self.get_bar_close_date_time(dt_open)  # Дата и время закрытия бара
        if self.p.sessionend != datetime_time(23, 59, 59, 999990) and dt_close.time() > self.p.sessionend:  # Если задано время окончания сессии и закрытие бара после этого времени
            self.logger.warning(f'Дата/время открытия бара {dt_open} после окончания торговой сессии {self.p.sessionend}')
            self.dt_last_open = dt_open  # Запоминаем дату/время открытия пришедшего бара для будущих сравнений
            return False  # то бар не соответствует условиям выборки
        if not self.p.four_price_doji and bar['high'] == bar['low']:  # Если не пропускаем дожи 4-х цен, но такой бар пришел
            self.logger.warning(f'Бар {dt_open} - дожи 4-х цен')
            self.dt_last_open = dt_open  # Запоминаем дату/время открытия пришедшего бара для будущих сравнений
            return False  # то бар не соответствует условиям выборки
        time_market_now = self.get_finam_date_time_now()  # Текущее биржевое время
        if dt_close > time_market_now and time_market_now.time() < self.p.sessionend:  # Если время закрытия бара еще не наступило на бирже, и сессия еще не закончилась
            self.logger.warning(f'Дата/время {dt_close} закрытия бара на {dt_open} еще не наступило')
            return False  # то бар не соответствует условиям выборки
        self.dt_last_open = dt_open  # Запоминаем дату/время открытия пришедшего бара для будущих сравнений
        return True  # В остальных случаях бар соответствуем условиям выборки

    def save_bar_to_file(self, bar, writer=None) -> None:
        """Сохранение бара в конец файла"""
        if writer is not None:
            get_writer = contextlib.nullcontext(writer)
        else:
            get_writer = self._with_history_bars_file_writer()

        with get_writer as writer:
            row = _csv_row_from_bar(self.symbol, bar)
            writer.writerow(row)  # Записываем бар в конец файла

    @contextlib.contextmanager
    def _with_history_bars_file_writer(self) -> Generator[csv.DictWriter, None, None]:
        write_header = False
        if not os.path.isfile(self.file_name):
            self.logger.warning(f'Файл {self.file_name} не найден и будет создан')
            write_header = True

        with open(self.file_name, 'a', newline='') as file:  # Создаем файл
            writer = csv.DictWriter(file, fieldnames=_FileField.ALL)
            if write_header:
                writer.writeheader()

            yield writer

    def get_finam_date_time_now(self) -> datetime:
        """Текущая дата и время МСК"""
        # TODO Получить текущее дату и время с Финама, когда появится в API
        return datetime.now(self.store.provider.tz_msk).replace(tzinfo=None)

    def request_closed_bar(self, interval):
        time.sleep(0.5)
        bars = self.request_bar(interval)
        if not bars or not bars['candles']:
            return

        time.sleep(0.5)
        for attempt in poll_timer(start_delay=0.5):
            if attempt > 0:
                self.logger.debug(f'waiting for bar stopped changing #{attempt}')

            new_bars = self.request_bar(interval)
            if new_bars and new_bars['candles'] == bars['candles']:
                bars = new_bars
                break

            self.logger.warning(f'Бар не совпадает:\n{bars["candles"]} != \n{new_bars["candles"]}')
            bars = new_bars

        return bars

    def request_bar(self, interval):
        if self.intraday:
            candles_getter = self.store.provider.get_intraday_candles
        else:
            candles_getter = self.store.provider.get_day_candles

        bars = None
        for attempt in poll_timer(max_tries=20):
            self.logger.debug(f'try to get bar #{attempt}')

            resp = candles_getter(self.board, self.symbol, self.finam_timeframe, interval)
            bars = MessageToDict(resp, always_print_fields_with_no_presence=True)
            if bars and bars.get('candles'):
                break

        return bars


def _bar_from_csv_row(row: dict) -> dict:
    return {
        'datetime': datetime.strptime(f'{row[_FileField.DATE]} {row[_FileField.TIME]}', '%Y%m%d %H%M%S', ),
        'open': float(row[_FileField.OPEN]),
        'high': float(row[_FileField.HIGH]),
        'low': float(row[_FileField.LOW]),
        'close': float(row[_FileField.CLOSE]),
        'volume': int(row[_FileField.VOL]),
    }


def _csv_row_from_bar(symbol: str, bar: dict) -> dict:
    return {
        _FileField.TICKER: symbol,
        _FileField.DATE: bar['datetime'].strftime('%Y%m%d'),
        _FileField.TIME: bar['datetime'].strftime('%H%M%S'),
        _FileField.OPEN: bar['open'],
        _FileField.HIGH: bar['high'],
        _FileField.LOW: bar['low'],
        _FileField.CLOSE: bar['close'],
        _FileField.VOL: bar['volume'],
    }


def poll_timer(max_tries=10, start_delay=0.1, max_delay=2, delay_multiplier=1.5):
    delay = start_delay
    for attempt in range(max_tries):
        yield attempt
        time.sleep(delay)
        delay *= delay_multiplier
        delay = min(delay, max_delay)
