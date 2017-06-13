from multicorn import ForeignDataWrapper
import urllib
from icalendar import Calendar, Event

class ICalFdw(ForeignDataWrapper):

    def __init__(self, options, columns):
        super(ICalFdw, self).__init__(options, columns)
        self.url = options.get('url', None)
	self.columns = columns

    def execute(self, quals, columns):
	ical_file = urllib.urlopen(self.url).read()
	cal = Calendar.from_ical(ical_file)
        for v in cal.walk('vevent'):
            e = Event(v)
	    line = {}
            for column_name in self.columns:
                if e.has_key(column_name):
                    if column_name == 'rrule':
                        line[column_name] = e[column_name].to_ical()
                    elif column_name == 'exdate':
                        dates = []
                        try:
                            for d in e[column_name]:
                                dates.append(str(d.dts[0].dt))
                        except TypeError:
                            dates.append(str(e[column_name].dts[0].dt))
                        line[column_name] = "{" + ",".join(dates) + "}"
                    else:
                        line[column_name] = e.decoded(column_name)

            yield line
