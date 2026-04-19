from dataclasses import dataclass

from app.config import Settings
from app.db.repo import Repo
from app.services.link_builder import build_offer_link
from app.validators import validate_full_name, validate_phone


@dataclass
class UserSessionData:
    scenario_code: str
    full_name: str | None = None
    phone: str | None = None
    consent_accepted: bool = False


class UserFlowService:
    def __init__(self, repo: Repo, settings: Settings):
        self.repo = repo
        self.settings = settings

    def start_scenario(self, scenario_code: str):
        scenario = self.repo.get_scenario_by_code(scenario_code)
        if not scenario:
            raise ValueError("Сценарий не найден")
        return scenario

    def validate_profile(self, full_name: str, phone: str) -> None:
        if not validate_full_name(full_name):
            raise ValueError("Укажите корректные ФИО")
        if not validate_phone(phone):
            raise ValueError("Укажите корректный номер телефона")

    def issue_personal_link(
        self,
        user_id: int,
        scenario_code: str,
        full_name: str,
        phone: str,
        *,
        max_name: str | None = None,
        max_username: str | None = None,
    ) -> str:
        self.validate_profile(full_name=full_name, phone=phone)
        scenario = self.start_scenario(scenario_code=scenario_code)
        subid = self.repo.next_subid(offer_id=scenario.offer_id)
        final_link = build_offer_link(offer=scenario.offer, subid_value=subid)
        self.repo.create_lead(
            user_id=user_id,
            scenario_id=scenario.id,
            offer_id=scenario.offer_id,
            full_name=full_name,
            phone=phone,
            subid_value=subid,
            consent_accepted=True,
            max_name=max_name,
            max_username=max_username,
        )
        return final_link

    def policy_url(self) -> str:
        return self.repo.effective_personal_data_policy_url(self.settings.personal_data_policy_url)
