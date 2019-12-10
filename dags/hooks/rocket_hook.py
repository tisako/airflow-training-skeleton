from airflow.hooks.http_hook import HttpHook


class RocketHook(HttpHook):
    def get_lauches(self, startdate, enddate):
        return HttpHook.run(self, f"https://launchlibrary.net/1.4/launch?startdate={startdate}&enddate={enddate}")
