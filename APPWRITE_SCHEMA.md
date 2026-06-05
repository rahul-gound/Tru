# Appwrite Schema Definition

## Prerequisites
- Appwrite project with API key that can manage databases/collections/documents.
- One database (example: `streaming`).
- Two collections: `Movies` and `Issues`.

## Collection: `Movies`
Create the following attributes:

| Attribute | Type | Required | Notes |
| --- | --- | --- | --- |
| `title` | String | Yes | Add index for instant title lookup/search |
| `rating` | Float | Yes | Decimal movie rating |
| `poster_image` | String | Yes | Public poster URL |
| `hf_stream_url` | String | Yes | Edge stream endpoint (Hugging Face Space URL) |
| `mobile_fallback_url` | String | Yes | Origin fallback stream URL |

Indexes:
- `title_idx` (key: `title`, type: `fulltext`) for `Query.search('title', ...)`.

## Collection: `Issues`
Create the following attributes:

| Attribute | Type | Required | Notes |
| --- | --- | --- | --- |
| `error_type` | String | Yes | e.g. `EDGE_UNAVAILABLE` |
| `severity` | String | Yes | e.g. `Critical` |
| `timestamp` | String | Yes | ISO-8601 timestamp |

## Recommended Permissions
- `Movies`: read for users/guests, write for admins.
- `Issues`: create for clients, read/write for admins only.

## Optional Collection: `Views` (for one-time view tracking)
Only required if `viewsCollectionId` is configured in `index.html`.

Suggested attributes:
- `movie_id` (string)
- `timestamp` (string, ISO-8601)
