# CouchDBViewsUpdateProcessor
// Java app

// You can change your dbnames
// my couchdb has 3 dbs "cashouts" / "transactions_aaa" / "cashedout_transactions_aaa"
// this app goes through all dbs in the couchdb and check names and hit views (query the view with limit=1)
// to keep the views up to date

// here's viewsinfo.txt example
// dbname (partial), design doc name, view name
cashouts, _design/install, terminalid_date_docid
cashedout_transactions, _design/install, terminalid_date_docid
transactions, _design/install, terminalid_date_docid

// here's app usage
// jar file and the viewsinfo.txt are in the same directory
// arguments : serveraddress:port, username, password, timeout, viewsinfo to keep updated, # of process
java -jar GeneralTouchViews_with_maxProcess.jar serveraddress:port username password timeout viewsinfo.txt 8
