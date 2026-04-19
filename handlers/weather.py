import aiohttp
import json
import os
from collections import defaultdict
from datetime import datetime
from typing import Optional

# Константы для направлений ветра
WIND_DIRECTIONS = [
    "северный", "северо-восточный", "восточный", "юго-восточный",
    "южный", "юго-западный", "западный", "северо-западный"
]

# Названия дней недели (русские)
DAYS_RU = ["ПН", "ВТ", "СР", "ЧТ", "ПТ", "СБ", "ВС"]

# Текстовые описания погоды (без эмодзи)
WEATHER_CODES = {
    0: "Ясное небо",
    1: "Преимущественно ясно",
    2: "Переменная облачность",
    3: "Пасмурно",
    45: "Туман",
    48: "Туман с изморозью",
    51: "Слабая морось",
    53: "Умеренная морось",
    55: "Сильная морось",
    56: "Слабая ледяная морось",
    57: "Сильная ледяная морось",
    61: "Небольшой дождь",
    63: "Умеренный дождь",
    65: "Сильный дождь",
    66: "Слабый ледяной дождь",
    67: "Сильный ледяной дождь",
    71: "Небольшой снег",
    73: "Умеренный снег",
    75: "Сильный снег",
    77: "Снежная крупа",
    80: "Кратковременный дождь",
    81: "Ливневый дождь",
    82: "Сильный ливень",
    85: "Слабый снегопад",
    86: "Сильный снегопад",
    95: "Гроза",
    96: "Гроза с градом",
    99: "Сильная гроза с градом"
}

# Путь к файлу для хранения городов пользователей
USER_CITIES_FILE = "db/user_cities.json"


def load_user_cities() -> dict:
    """Загружает словарь {user_id: city} из JSON-файла."""
    if not os.path.exists(USER_CITIES_FILE):
        return {}
    try:
        with open(USER_CITIES_FILE, "r", encoding="utf-8") as f:
            return json.load(f)
    except (json.JSONDecodeError, IOError):
        return {}


def save_user_cities(data: dict):
    """Сохраняет словарь {user_id: city} в JSON-файл."""
    os.makedirs(os.path.dirname(USER_CITIES_FILE), exist_ok=True)
    with open(USER_CITIES_FILE, "w", encoding="utf-8") as f:
        json.dump(data, f, ensure_ascii=False, indent=2)


def get_user_city(user_id: str) -> Optional[str]:
    """Возвращает сохранённый город для пользователя или None."""
    data = load_user_cities()
    return data.get(user_id)


def set_user_city(user_id: str, city: str):
    """Сохраняет город для пользователя."""
    data = load_user_cities()
    data[user_id] = city
    save_user_cities(data)


def deg_to_wind(deg: float) -> str:
    """Преобразует градусы в текстовое направление ветра."""
    idx = round(deg / 45) % 8
    return WIND_DIRECTIONS[idx]


def format_temp(t: float) -> str:
    """Форматирует температуру со знаком и градусами."""
    t = round(t)
    return f"+{t}°C" if t > 0 else f"{t}°C"


async def get_coordinates(city: str):
    """Получает координаты (lat, lon) и нормализованное название города через Nominatim."""
    async with aiohttp.ClientSession() as session:
        async with session.get(
            "https://nominatim.openstreetmap.org/search",
            params={"q": city, "format": "json", "limit": 5, "addressdetails": 1},
            headers={"User-Agent": "weather-bot/1.0"},
            timeout=10
        ) as r:
            data = await r.json()

    if not data:
        raise ValueError(city)

    place_types = [
        "city", "town", "village", "hamlet", "isolated_dwelling",
        "farm", "allotments", "neighbourhood", "suburb", "quarter",
        "borough", "municipality", "administrative"
    ]

    for item in data:
        if item.get("class") == "place" and item.get("type") in place_types:
            address = item.get("address")
            if address and "city" in address:
                city_name = address["city"]
            else:
                city_name = item["display_name"].split(",")[0]
            return float(item["lat"]), float(item["lon"]), city_name

    # fallback на первый результат
    i = data[0]
    address = i.get("address")
    if address and "city" in address:
        city_name = address["city"]
    else:
        city_name = i["display_name"].split(",")[0]
    return float(i["lat"]), float(i["lon"]), city_name


async def get_weather(lat: float, lon: float):
    """Запрашивает текущую погоду и почасовой прогноз с open-meteo."""
    async with aiohttp.ClientSession() as session:
        async with session.get(
            "https://api.open-meteo.com/v1/forecast",
            params={
                "latitude": lat,
                "longitude": lon,
                "current_weather": "true",
                "hourly": "temperature_2m,weathercode,pressure_msl",
                "timezone": "auto",
                "windspeed_unit": "ms"
            },
            timeout=10
        ) as r:
            return await r.json()


def build_data(city: str, data: dict) -> dict:
    """
    Из сырых данных API строит структурированный словарь с прогнозом:
    - текущая погода
    - сводка на сегодня (день/ночь)
    - прогноз на следующие 5 дней (день/ночь)
    """
    days = defaultdict(list)
    for t, temp, code in zip(
        data["hourly"]["time"],
        data["hourly"]["temperature_2m"],
        data["hourly"]["weathercode"]
    ):
        d, h = t.split("T")
        days[d].append((int(h[:2]), temp, code))

    # прогноз на 5 дней (со 2-го по 6-й день от сегодня)
    forecast = []
    for d in sorted(days)[1:6]:
        day = [(t, c) for h, t, c in days[d] if 10 <= h <= 17]
        night = [(t, c) for h, t, c in days[d] if h <= 6 or h >= 22]
        if not day or not night:
            continue

        # наиболее частый код погоды днём и ночью
        day_codes = [c for _, c in day]
        night_codes = [c for _, c in night]
        day_code = max(set(day_codes), key=day_codes.count)
        night_code = max(set(night_codes), key=night_codes.count)

        forecast.append({
            "day": DAYS_RU[datetime.fromisoformat(d).weekday()],
            "day_temp": round(sum(t for t, _ in day) / len(day)),
            "night_temp": round(sum(t for t, _ in night) / len(night)),
            "day_desc": WEATHER_CODES.get(day_code, "—"),
            "night_desc": WEATHER_CODES.get(night_code, "—")
        })

    cur = data["current_weather"]
    hour = int(cur["time"].split("T")[1][:2])
    is_night = hour < 6 or hour >= 21  # не используется для текста, но оставим

    # давление на текущий час
    pressure = None
    try:
        time_idx = data["hourly"]["time"].index(cur["time"])
        pressure = round(data["hourly"]["pressure_msl"][time_idx])
    except (ValueError, IndexError):
        if data["hourly"]["time"]:
            pressure = round(data["hourly"]["pressure_msl"][0])

    # сводка на сегодня (если есть данные)
    current_date = cur["time"].split("T")[0]
    today_hours = days.get(current_date, [])
    today_summary = None
    if today_hours:
        day_data = [(t, c) for h, t, c in today_hours if 10 <= h <= 17]
        night_data = [(t, c) for h, t, c in today_hours if h >= 22 or h < 6]
        if day_data and night_data:
            day_codes = [c for _, c in day_data]
            night_codes = [c for _, c in night_data]
            today_summary = {
                "day_temp": round(sum(t for t, _ in day_data) / len(day_data)),
                "night_temp": round(sum(t for t, _ in night_data) / len(night_data)),
                "day_desc": WEATHER_CODES.get(max(set(day_codes), key=day_codes.count), "—"),
                "night_desc": WEATHER_CODES.get(max(set(night_codes), key=night_codes.count), "—")
            }

    return {
        "city": city,
        "temp": round(cur["temperature"]),
        "desc": WEATHER_CODES.get(cur["weathercode"], "—"),
        "wind_speed": round(cur.get("windspeed", 0), 1),
        "wind_dir": deg_to_wind(cur.get("winddirection", 0)),
        "pressure": round(pressure * 0.750062) if pressure else None,  # мм рт. ст.
        "today_summary": today_summary,
        "forecast": forecast
    }


def build_text_forecast(d: dict) -> str:
    """
    Формирует текстовый прогноз из структурированных данных.
    Возвращает многострочную строку без эмодзи и форматирования.
    """
    lines = []
    lines.append(f"Погода в {d['city']}:")

    # текущая погода
    current = (f"Сейчас: {format_temp(d['temp'])}, {d['desc']}, "
               f"ветер {d['wind_speed']} м/с ({d['wind_dir']})")
    if d['pressure']:
        current += f", давление {d['pressure']} мм.рт.ст"
    lines.append(current)
    lines.append("")

    # сводка на сегодня
    if d.get("today_summary"):
        ts = d["today_summary"]
        lines.append(f"Сегодня: день {format_temp(ts['day_temp'])}, {ts['day_desc']}; "
                     f"ночь {format_temp(ts['night_temp'])}, {ts['night_desc']}")
        lines.append("")

    # прогноз на ближайшие дни
    if d["forecast"]:
        lines.append("Прогноз на ближайшие дни:")
        for f in d["forecast"]:
            lines.append(f"{f['day']}: день {format_temp(f['day_temp'])}, {f['day_desc']}; "
                         f"ночь {format_temp(f['night_temp'])}, {f['night_desc']}")
    else:
        lines.append("Прогноз на ближайшие дни отсутствует.")

    return "\n".join(lines)


def setup(handler):
    """Регистрация команды weather в ICQ боте."""
    handler.register_command("weather", weather_command)


async def weather_command(bot, user_id: str, args: str) -> str:
    """
    Обработчик команды /weather [город]
    Возвращает текстовый прогноз погоды.
    """
    city = args.strip() if args else None

    # Если город не передан, берём сохранённый
    if not city:
        city = get_user_city(user_id)
        if not city:
            return ("Использование: /weather [город]\n"
                    "Пример: /weather Москва\n"
                    "После первого запроса город будет сохранён.")

    try:
        lat, lon, city_name = await get_coordinates(city)
        weather_data = await get_weather(lat, lon)
        data = build_data(city_name, weather_data)
        text = build_text_forecast(data)

        # Сохраняем город для пользователя
        set_user_city(user_id, city)

        return text

    except ValueError:
        return f"Город '{city}' не найден. Попробуйте другой."
    except Exception as e:
        # В реальном проекте лучше логировать ошибку
        return f"Не удалось получить погоду. Ошибка: {e}"