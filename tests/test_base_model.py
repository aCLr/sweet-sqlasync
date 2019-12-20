from utils import Model, compile_statement


def test_query():
    query = Model.query.with_entities(Model.id)
    assert compile_statement(query.statement) == "SELECT models.id \nFROM models"
