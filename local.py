from werkzeug.local import Local, LocalManager

local = Local()
local_manager = LocalManager()

request = local('request')