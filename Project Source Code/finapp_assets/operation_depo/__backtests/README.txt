:: README file for backtests
-
The file < .bt_config > holds the current configuration of the engine (options set by user) and
the file < .bt_resource_core > holds information or data needed by backtests to operate. Any 
additional resource files added (this is a beta framework being tested) will be addressed in the 
Appendix (below). The files with a preceding " __$ " are the DEFAULT files and are used to restore
engine settings if needed - YOU SHOULD NOT UNDER ANY CIRCUMSTANCES delete these files, and
modifying them is NOT RECOMMENDED without a solid understanding of the overall design. 
-
The { __configurations } directory holds saved backtest settings for the core engine as well as
the rule sets and respective asset universes & portfolios used. 
-
The { __records } directory holds the full result summary data for saved backtests. 
-
The [ bt_logfile.xlsx ] file holds the full accounting of backtest operations (all tests, saved or
not, with overall performance and file paths to saved resources (if applicable).
-

:: Appendix
-	