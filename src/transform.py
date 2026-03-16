










multiplier = 3
def make_sythetic_events(raw_events,multiplier:int) -> dict[str,any]:
    out = []
    for _ in range(multiplier):
        for e in raw_events:
            new_e = new_e["is_synthetic"] = True
            new_e["synthetic_id"] = str(uuid.uuid4())
            new_e["generated_at"] = datetime.now(timezone.utc).isoformat()
            out.append(new_e)
    return out

