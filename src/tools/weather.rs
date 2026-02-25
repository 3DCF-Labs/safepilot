#![allow(dead_code)]

use anyhow::{anyhow, Context, Result};
use serde::Deserialize;

const OPENWEATHER_API_URL: &str = "https://api.openweathermap.org/data/2.5";

#[derive(Debug, Clone, Deserialize)]
pub struct CurrentWeather {
    pub name: String,
    pub weather: Vec<WeatherCondition>,
    pub main: MainWeather,
    pub wind: Wind,
    pub clouds: Clouds,
    pub sys: Sys,
    #[serde(default)]
    pub rain: Option<Precipitation>,
    #[serde(default)]
    pub snow: Option<Precipitation>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct WeatherCondition {
    pub id: i32,
    pub main: String,
    pub description: String,
    pub icon: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct MainWeather {
    pub temp: f64,
    pub feels_like: f64,
    pub temp_min: f64,
    pub temp_max: f64,
    pub pressure: i32,
    pub humidity: i32,
}

#[derive(Debug, Clone, Deserialize)]
pub struct Wind {
    pub speed: f64,
    #[serde(default)]
    pub deg: Option<i32>,
    #[serde(default)]
    pub gust: Option<f64>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct Clouds {
    pub all: i32,
}

#[derive(Debug, Clone, Deserialize)]
pub struct Sys {
    pub country: String,
    pub sunrise: i64,
    pub sunset: i64,
}

#[derive(Debug, Clone, Deserialize)]
pub struct Precipitation {
    #[serde(rename = "1h", default)]
    pub one_hour: Option<f64>,
    #[serde(rename = "3h", default)]
    pub three_hour: Option<f64>,
}

#[derive(Debug, Clone, Deserialize)]
pub struct Forecast {
    pub list: Vec<ForecastItem>,
    pub city: City,
}

#[derive(Debug, Clone, Deserialize)]
pub struct ForecastItem {
    pub dt: i64,
    pub main: MainWeather,
    pub weather: Vec<WeatherCondition>,
    pub wind: Wind,
    pub dt_txt: String,
}

#[derive(Debug, Clone, Deserialize)]
pub struct City {
    pub name: String,
    pub country: String,
}

pub struct WeatherClient {
    api_key: String,
    client: reqwest::Client,
}

impl WeatherClient {
    pub fn new(api_key: &str) -> Self {
        Self {
            api_key: api_key.to_string(),
            client: reqwest::Client::new(),
        }
    }

    pub async fn current(&self, city: &str, units: &str) -> Result<CurrentWeather> {
        let url = format!(
            "{}/weather?q={}&units={}&appid={}",
            OPENWEATHER_API_URL,
            urlencoding::encode(city),
            units,
            self.api_key
        );

        let response = self
            .client
            .get(&url)
            .send()
            .await
            .context("Failed to fetch weather")?;

        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            return Err(anyhow!("OpenWeather API error: {} - {}", status, body));
        }

        response
            .json()
            .await
            .context("Failed to parse weather data")
    }

    pub async fn forecast(&self, city: &str, units: &str) -> Result<Forecast> {
        let url = format!(
            "{}/forecast?q={}&units={}&appid={}",
            OPENWEATHER_API_URL,
            urlencoding::encode(city),
            units,
            self.api_key
        );

        let response = self
            .client
            .get(&url)
            .send()
            .await
            .context("Failed to fetch forecast")?;

        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            return Err(anyhow!("OpenWeather API error: {} - {}", status, body));
        }

        response
            .json()
            .await
            .context("Failed to parse forecast data")
    }

    pub async fn current_by_coords(
        &self,
        lat: f64,
        lon: f64,
        units: &str,
    ) -> Result<CurrentWeather> {
        let url = format!(
            "{}/weather?lat={}&lon={}&units={}&appid={}",
            OPENWEATHER_API_URL, lat, lon, units, self.api_key
        );

        let response = self
            .client
            .get(&url)
            .send()
            .await
            .context("Failed to fetch weather")?;

        if !response.status().is_success() {
            let status = response.status();
            let body = response.text().await.unwrap_or_default();
            return Err(anyhow!("OpenWeather API error: {} - {}", status, body));
        }

        response
            .json()
            .await
            .context("Failed to parse weather data")
    }
}

pub fn format_current(weather: &CurrentWeather, units: &str) -> String {
    let temp_unit = if units == "metric" { "°C" } else { "°F" };
    let speed_unit = if units == "metric" { "m/s" } else { "mph" };

    let condition = weather
        .weather
        .first()
        .map(|w| format!("{} ({})", w.main, w.description))
        .unwrap_or_else(|| "Unknown".to_string());

    let emoji = weather_emoji(weather.weather.first().map(|w| w.id).unwrap_or(800));

    format!(
        "{} {} {}, {}\n\
         🌡️ {:.1}{} (feels like {:.1}{})\n\
         💨 Wind: {:.1} {}\n\
         💧 Humidity: {}%\n\
         ☁️ Clouds: {}%",
        emoji,
        weather.name,
        weather.sys.country,
        condition,
        weather.main.temp,
        temp_unit,
        weather.main.feels_like,
        temp_unit,
        weather.wind.speed,
        speed_unit,
        weather.main.humidity,
        weather.clouds.all
    )
}

pub fn format_forecast(forecast: &Forecast, units: &str) -> String {
    let temp_unit = if units == "metric" { "°C" } else { "°F" };

    let mut lines = vec![format!(
        "📅 Forecast for {}, {}:",
        forecast.city.name, forecast.city.country
    )];

    for item in forecast.list.iter().take(8) {
        let condition = item
            .weather
            .first()
            .map(|w| &w.main)
            .map(|s| s.as_str())
            .unwrap_or("?");
        let emoji = weather_emoji(item.weather.first().map(|w| w.id).unwrap_or(800));

        let time = item
            .dt_txt
            .split(' ')
            .nth(1)
            .unwrap_or("")
            .trim_end_matches(":00");

        lines.push(format!(
            "  {} {} {:.0}{} {}",
            time, emoji, item.main.temp, temp_unit, condition
        ));
    }

    lines.join("\n")
}

fn weather_emoji(condition_id: i32) -> &'static str {
    match condition_id {
        200..=232 => "⛈️", // Thunderstorm
        300..=321 => "🌧️", // Drizzle
        500..=531 => "🌧️", // Rain
        600..=622 => "🌨️", // Snow
        701..=781 => "🌫️", // Atmosphere (fog, mist, etc.)
        800 => "☀️",       // Clear
        801 => "🌤️",       // Few clouds
        802 => "⛅",       // Scattered clouds
        803..=804 => "☁️", // Cloudy
        _ => "🌡️",
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn test_weather_emoji() {
        assert_eq!(weather_emoji(800), "☀️");
        assert_eq!(weather_emoji(500), "🌧️");
        assert_eq!(weather_emoji(600), "🌨️");
    }
}
