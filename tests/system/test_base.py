from mssqlbeat import BaseTest

import os


class Test(BaseTest):

    def test_base(self):
        """
        Basic test with exiting Mssqlbeat normally
        """
        self.render_config_template(
            path=os.path.abspath(self.working_dir) + "/log/*"
        )

        mssqlbeat_proc = self.start_beat()
        self.wait_until(lambda: self.log_contains("mssqlbeat is running"))
        exit_code = mssqlbeat_proc.kill_and_wait()
        assert exit_code == 0
