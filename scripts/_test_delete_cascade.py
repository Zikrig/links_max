"""One-off: verify Offer delete cascades. Run: python scripts/_test_delete_cascade.py"""
import os
import tempfile

from sqlalchemy import create_engine, event
from sqlalchemy.orm import Session

from app.db.database import Base
from app.db import models
from app.db.repo import Repo


def main() -> None:
    fd, path = tempfile.mkstemp(suffix=".sqlite3")
    os.close(fd)
    engine = create_engine(f"sqlite:///{path}", future=True)

    @event.listens_for(engine, "connect")
    def _fk(dbapi_connection, _):
        dbapi_connection.execute("PRAGMA foreign_keys=ON")

    Base.metadata.create_all(bind=engine)
    s = Session(engine)
    repo = Repo(s)
    p = repo.create_platform("P1")
    o = repo.create_offer(p.id, "O1", "https://x.com", "s")
    sc = repo.create_scenario(o.id, "code1", "T")
    repo.create_or_update_bot_link(sc.id, "https://max.ru/x")
    repo.add_scenario_channel(sc.id, -100, "ch")
    repo.create_lead(1, sc.id, o.id, "0001", "n", "p")

    repo.delete_offer(o.id)

    assert s.query(models.Offer).count() == 0
    assert s.query(models.Scenario).count() == 0
    assert s.query(models.Lead).count() == 0
    assert s.query(models.BotLink).count() == 0
    assert s.query(models.ScenarioChannel).count() == 0
    print("delete_offer cascade OK")

    repo.delete_platform(p.id)
    assert s.query(models.Platform).count() == 0
    print("delete_platform cascade OK")

    s.close()
    os.unlink(path)


if __name__ == "__main__":
    main()
