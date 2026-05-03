from __future__ import annotations

from dataclasses import dataclass
from typing import Any
from urllib.parse import urlencode

import requests


@dataclass(frozen=True)
class JellyfinApi:
    base_url: str
    api_key: str

    def headers(self) -> dict[str, str]:
        return {"X-Emby-Token": self.api_key, "Content-Type": "application/json"}

    def get_users(self) -> list[dict[str, Any]]:
        r = requests.get(f"{self.base_url}/Users", headers=self.headers(), timeout=15)
        r.raise_for_status()
        return r.json()

    def create_user(self, username: str, password: str) -> dict[str, Any]:
        r = requests.post(
            f"{self.base_url}/Users/New",
            json={"Name": username, "Password": password},
            headers=self.headers(),
            timeout=15,
        )
        r.raise_for_status()
        return r.json()

    def get_user(self, user_id: str) -> dict[str, Any]:
        r = requests.get(f"{self.base_url}/Users/{user_id}", headers=self.headers(), timeout=15)
        r.raise_for_status()
        return r.json()

    def delete_user(self, user_id: str) -> None:
        r = requests.delete(f"{self.base_url}/Users/{user_id}", headers=self.headers(), timeout=15)
        if r.status_code in (204, 404):
            return
        if r.status_code != 204:
            r.raise_for_status()

    def update_policy(self, user_id: str, policy: dict[str, Any]) -> None:
        r = requests.post(f"{self.base_url}/Users/{user_id}/Policy", json=policy, headers=self.headers(), timeout=15)
        if r.status_code != 204:
            r.raise_for_status()

    def set_disabled(self, user_id: str, disabled: bool) -> None:
        user = self.get_user(user_id)
        policy = user.get("Policy", {})
        policy["IsDisabled"] = bool(disabled)
        self.update_policy(user_id, policy)

    def set_initial_policy(self, user_id: str) -> None:
        user = self.get_user(user_id)
        policy = user.get("Policy", {})
        policy["EnableContentDownloading"] = False
        policy["MaxActiveSessions"] = 3
        policy["IsDisabled"] = False
        self.update_policy(user_id, policy)

    def refresh_library(
        self,
        library_id: str,
        *,
        recursive: bool = True,
        image_refresh_mode: str = "Default",
        metadata_refresh_mode: str = "Default",
        replace_all_images: bool = False,
        regenerate_trickplay: bool = False,
        replace_all_metadata: bool = False,
    ) -> None:
        query = urlencode(
            {
                "Recursive": "true" if recursive else "false",
                "ImageRefreshMode": image_refresh_mode,
                "MetadataRefreshMode": metadata_refresh_mode,
                "ReplaceAllImages": "true" if replace_all_images else "false",
                "RegenerateTrickplay": "true" if regenerate_trickplay else "false",
                "ReplaceAllMetadata": "true" if replace_all_metadata else "false",
            }
        )
        url = f"{self.base_url}/Items/{library_id}/Refresh?{query}"
        r = requests.post(url, headers=self.headers(), timeout=30)
        if r.status_code != 204:
            r.raise_for_status()

    def refresh_library_default(self, library_id: str) -> None:
        self.refresh_library(library_id)

    def get_devices(self) -> list[dict[str, Any]]:
        r = requests.get(f"{self.base_url}/Devices", headers=self.headers(), timeout=15)
        r.raise_for_status()
        data = r.json()
        items = data.get("Items", []) if isinstance(data, dict) else []
        return items if isinstance(items, list) else []

    def delete_device(self, device_id: str) -> bool:
        r = requests.delete(f"{self.base_url}/Devices?Id={device_id}", headers=self.headers(), timeout=15)
        if r.status_code in (200, 204, 404):
            return True
        r.raise_for_status()
        return False
