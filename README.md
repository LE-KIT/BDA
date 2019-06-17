# Preprocessing

## Schritte Preprocessing:
- Fill Blanks/Nans
- Outlier Detection 

## Ideen Feature Extraction: 
- Wochentag (1-7)
- Woche (1-55)
- Moving Average der letzten x Tage aus jeweiligem Land 
- Moving Average der letzten x Tage Gesamt Netto-Exporte

## Neue Datenquellen:
- Wetterdaten
- Ausfälle Kraftwerke  



# Nützliche Git Befehle

- Änderungen Hinzufügen:
	- git add geändertes_file
	- git commit 
	- git push 

- Änderungen runterlagen:
	- git pull

- Repo Status etc.
	- git status
	- git log 

- Neuer Branch auschecken
	- git checkout -b neuer_branch
	- git push -u origin neuer_branch 

- Manchmal nützlich
	- git commit --amend : Änderungen letztem commit hinzufügen
	- git rebase : Am besten Doku anschauen. Kann man commits zusammenfassen, branches rebasen auf neue Verzweigung etc. 