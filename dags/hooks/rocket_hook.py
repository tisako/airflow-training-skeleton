from airflow.hooks.http_hook import HttpHook


class RocketHook(HttpHook):
    def __init__(
            self,
            method='GET',
            http_conn_id='rocket_connection'
    ):
        super().__init__(method, http_conn_id)

    def get_lauches(self, startdate, enddate):
        print(enddate)
        return super().run(endpoint=f"?startdate={startdate}&enddate={enddate}")
