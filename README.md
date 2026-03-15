# Spase Simple API

A self-hosted JSON API for astrophysical events, updated hourly via GitHub Actions and served as a static file on GitHub Pages.

**API endpoint:** https://alieron.github.io/spase/events.json


## Sources

| Type | Source |
|------|--------|
| `solar_flare` | NASA DONKI |
| `coronal_mass_ejection` | NASA DONKI |
| `gravitational_wave` | LIGO/Virgo GraceDB |

## API Schema

```json
{
  "meta": {
    "generated_at": "2024-01-15T12:05:00Z",
    "window_start":  "2024-01-08T12:05:00Z",
    "window_end":    "2024-01-15T12:05:00Z",
    "retention_days": 7,
    "total_events": 42,
    "event_counts": { "solar_flare": 12, "coronal_mass_ejection": 8, ... },
    "schema_version": "2.0"
  },
  "events": [
    {
      "id":          "flr-...",
      "type":        "solar_flare",
      "source":      "NASA DONKI",
      "time":        "2024-01-15T10:30:00Z",
      "title":       "Solar Flare X2.5",
      "description": "...",
      "severity":    "extreme",
      "metadata":    { ... },
      "url":         "https://..."
    }
  ]
}
```

## Local run

```bash
python aggregate.py
```

Requires Python 3.10+. No external dependencies.
