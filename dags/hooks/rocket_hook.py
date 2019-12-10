from airflow.hooks.http_hook import HttpHook


class RocketHook(HttpHook):
    def __init__(
            self,
            method='POST',
            http_conn_id='rocket_connection'
    ):
        super().__init__(method, http_conn_id)

    def get_lauches(self, startdate, enddate):
        return super().run(endpoint=f"https://launchlibrary.net/1.4/launch?startdate={startdate}&enddate={enddate}")
