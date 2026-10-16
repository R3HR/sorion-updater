# Auffindbarkeit: robots, Sitemap, llms.txt, statische Zahlen (16.10.2026)

**Ausgangslage:** Alle Seiten holten ihre Zahlen per JavaScript aus Supabase. Google indexiert das
nur eingeschraenkt, Bing kaum, und Antwortmaschinen sehen gar nichts. Fuer die ChatGPT-Suche ist
das besonders relevant, weil sie unter anderem auf den Bing-Index zurueckgreift.

## Was jetzt im Repo `sorion-ui` liegt

| Datei | Zweck |
|---|---|
| `robots.txt` | erlaubt alles ausser `/auth/` und `/profile.html`, nennt die Sitemap |
| `sitemap.xml` | 7 Seiten mit lastmod, taeglich neu datiert |
| `llms.txt` | Kurzbeschreibung fuer Sprachmodelle, mit den aktuellen Genauigkeitszahlen |
| `thresholds.html` | **die Zahlen als fester Text**, ohne JavaScript lesbar |
| `tools/build-static.mjs` | erzeugt die drei erzeugten Dateien aus oeffentlichen Endpunkten |
| `tools/indexnow.mjs` | meldet Aenderungen bei IndexNow (Bing, Yandex, Seznam, Naver) |
| `<32 Hex>.txt` | IndexNow-Schluessel, muss unter der Domain erreichbar bleiben |
| `.github/workflows/nightly.yml` | taeglich 06:20 UTC: neu erzeugen, committen, IndexNow melden |

**06:20 UTC** liegt bewusst nach dem Updater-Fenster (22:00 bis 05:00) und nach den Tages-Snapshots
(05:30 und 05:45).

## thresholds.html

Je Wettbewerb und Rarity: Punkte fuer Essence, Tiefe der bezahlten Raenge, Zahl der Aufstellungen,
Siegerscore, Anzahl ausgewerteter Spieltage. Median ueber die Saison, damit eine einzelne Woche
nichts verzerrt. Stand 16.10.: **48 Zeilen aus 21 abgeschlossenen Spieltagen**, dazu eine Tabelle
mit unserer Treffgenauigkeit gegen das guenstigste Angebot und JSON-LD als `Dataset`.

Cash-Zahlen bleiben aussen vor: Die RPC liefert sie anonym als null (Pro-Feature), damit ist die
statische Seite automatisch auf der freien Teilmenge.

## IndexNow

Kein Konto noetig, der Nachweis ist die Schluesseldatei unter der Domain. Nach jedem naechtlichen
Lauf werden die sechs wichtigsten Adressen gemeldet. **Die Schluesseldatei darf nicht geloescht
oder umbenannt werden**, sonst lehnt IndexNow kuenftige Meldungen ab.

## OFFEN: Anmeldung bei Google und Bing (nur Jonas)

Beides verlangt eine Anmeldung mit seinem Konto, das kann und darf ich nicht uebernehmen.

**Google Search Console**, empfohlener Weg (deckt alle Unterseiten ab):
1. search.google.com/search-console, Property-Typ **Domain**, `sorion.pro` eintragen.
2. Google zeigt einen TXT-Eintrag. Den beim Domain-Anbieter als DNS-TXT auf die Wurzel setzen.
3. Nach der Bestaetigung unter Sitemaps `https://sorion.pro/sitemap.xml` einreichen.
   Alternativ ohne DNS: Property-Typ URL-Praefix, Google gibt eine Datei `googleXXXX.html`.
   Die kann ich committen, dafuer brauche ich nur Dateiname und Inhalt.

**Bing Webmaster Tools:**
1. bing.com/webmasters, dort **Import aus Google Search Console** waehlen, das uebernimmt
   Property und Bestaetigung in zwei Klicks. Erst sinnvoll, wenn Schritt 1 oben erledigt ist.
2. Ohne Google-Weg: `BingSiteAuth.xml` herunterladen, Inhalt an mich, ich committe sie.

Danach in Bing einmal pruefen, ob IndexNow unter demselben Konto als verifiziert erscheint.
