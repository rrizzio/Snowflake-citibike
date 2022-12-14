from snowflake.snowpark.session import Session

def make_session(accountname, username, password, rolename):
    connection_parameters = {
        "account": accountname,
        "user": username,
        "password": password,
        "role": rolename
    }
    session = Session.builder.configs(connection_parameters).create()
    return session