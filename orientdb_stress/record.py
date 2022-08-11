class Record:
    def __init__(self, rid: str, record_id: int, prop_uq: int, prop_nuq: int, prop_ftx: str):
        self.rid = rid
        self.record_id = record_id
        self.prop_uq = prop_uq
        self.prop_nuq = prop_nuq
        self.prop_ftx = prop_ftx

    def __str__(self) -> str:
        return f"{self.__dict__}"

    def next_uq(self) -> "Record":
        return Record(self.rid, self.record_id, self.prop_uq + 1, self.prop_nuq, self.prop_ftx)

    def next_nuq(self) -> "Record":
        return Record(self.rid, self.record_id, self.prop_uq, self.prop_nuq + 1, self.prop_ftx)
