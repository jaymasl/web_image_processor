# Image Processing Script

This anonymized script automates the process of fetching, analyzing, and storing public images. 
It extracts EXIF metadata and tags from images, storing the results in a DuckDB database. 

## Features

- **Web Scraping**: Fetches image data using `requests` and Selenium.
- **Image Processing**: Extracts EXIF metadata and image tags with `Pillow` and `pillow_heif`.
- **Database Storage**: Stores image data, tags, and user comments in a DuckDB database.
- **Duplicate Detection**: Detects and avoids storing duplicate or similar images.
- **Logging & Error Handling**: Logs errors and retries failed requests, providing a robust process.

## Database Schema

The `images` table has the following structure:

| Column         | Type      | Description                         |
|----------------|-----------|-------------------------------------|
| `id`           | BIGINT    | Unique image identifier.            |
| `url`          | TEXT      | URL of the image.                   |
| `hash`         | TEXT      | Image hash.                         |
| `createdAt`    | TIMESTAMP | Image creation timestamp.           |
| `postId`       | BIGINT    | Related post ID.                    |
| `username`     | TEXT      | Uploader's username.                |
| `web_url`      | TEXT      | Web page URL of the image.          |
| `tags`         | TEXT[]    | Extracted tags.                     |
| `user_comment` | TEXT      | EXIF user comment.                  |