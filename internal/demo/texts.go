package demo

import (
	"fmt"
	"strings"
	"time"

	"github.com/Demonisreal/dmn-ledger/ledger"
)

const (
	sayKill       = "SIGKILL an n%d (PID %d), %s."
	sayKillLifted = "n%d startet nach %s s wieder (PID %d)."
	sayFreeze     = "SIGSTOP an n%d (PID %d), %s. Der Prozess lebt und rechnet nicht."
	sayThawed     = "SIGCONT an n%d nach %s s."
	sayCut        = "n%d ist von n%d und n%d getrennt, beide Richtungen (4 Links). Clients erreichen n%d weiter."
	sayHealed     = "Links von und zu n%d nach %s s wieder offen."
	sayTipped     = "Bit gekippt in %s: Byte %s von %s, Bit %d, 0x%02x wird 0x%02x."
	sayStarted    = "n%d gestartet (PID %d)."
	sayRepaired   = "n%d hat den Schaden beim Start erkannt und %d ms lang nicht abgestimmt. Der Leader hat das Log aufgefüllt; n%d stimmt wieder ab."
	sayTruncated  = "n%d ist ohne Reparatur gestartet: Das Bit lag in einem Schreibvorgang, den n%d noch nicht bestätigt hatte. Solche Enden schneidet der Knoten ab (ADR-009)."

	saySetup       = "Cluster %d aufgesetzt (%s): Cluster-ID %016x (dezimal %d, so nennen sie die Zeilen der Knoten), drei neue Schlüssel, PIDs %d, %d, %d."
	sayFunded      = "Sieben Konten angelegt, sechs davon mit je %s ausgestattet."
	saySetupFailed = "Aufsetzen gescheitert, Versuch %d von %d: %v"
	sayLight       = "Zählung: Summe %s."
	sayFull        = "Zählung: alle drei Knoten bei Eintrag %d, Zustand %s. %s Buchungen bestätigt, %s gefunden; %s abgelehnt, %s davon im Hauptbuch. Summe %s."
	sayAfter       = "Zählung nach der Störung: "
	sayLimit       = "%s Buchungen erreicht. Schlusszählung, dann wird neu aufgesetzt."
	sayRest        = "Niemand sieht zu. Die Last ruht."
	sayFinding     = "Befund %s: %s"
	sayEvidence    = "Daten und Logs der Knoten gesichert unter %s."
	saySkipped     = "%d weitere Zeilen von n%d"

	noteKill      = "abgeschossen, nach %s s gestartet"
	noteFreeze    = "eingefroren, nach %s s weiter"
	noteCut       = "abgetrennt, nach %s s wieder verbunden"
	noteFlip      = "Bit gekippt, %d ms Nicht-Wähler"
	noteTruncated = "Bit gekippt, ohne Reparatur gestartet"

	whyStart   = "nach dem Start"
	whyLimit   = "%s Buchungen erreicht"
	whyFinding = "nach einem Befund"

	silent = "antwortet nicht"
)

const (
	failExited     = "n%d endete, ohne dass das Steuerprogramm ihn beendet hat (%s). Die letzten Zeilen seines Logs:\n%s"
	failCall       = "c%d, Aufruf %d (%s): %v"
	failAsk        = "Zählung: %v"
	failOpen       = "%d Aufrufe sind %v nach dem Anhalten der Last noch offen"
	failApart      = "die Knoten sind sich %v nach dem Anhalten der Last nicht einig: %s"
	failLeaders    = "%d Leader; %s"
	failNode       = "n%d %s, Commit %d, angewendet %d, Kopf %d %x"
	failHashes     = "n%d und n%d haben bis Eintrag %d angewendet, mit den Hashes %x und %x"
	failRepair     = "n%d ist in Term %d in Reparatur, ohne dass ein Bit gekippt wurde"
	failRepairLine = "n%d meldet Reparatur, ohne dass ein Bit gekippt wurde: %s"
	failKey        = "n%d zeigt den Schlüssel %x, gestartet wurde er mit %x"
	failSum        = "Summe der sechs Konten %s statt %s"
	failSpent      = "Konto 9 hat %s im Soll statt %s"
	failSides      = "Soll %s und Haben %s über alle Konten"
	failPending    = "Konto %v hat offene Beträge: Soll %v, Haben %v"
	failAccounts   = "die Zählung fand %d der sieben Konten"
	failMissing    = "%d bestätigte Buchungen fehlen im Hauptbuch, die erste ist %d"
	failStrays     = "%d Buchungen im Hauptbuch sind abgelehnt oder nie geschickt worden, die erste ist %v"
	failStart      = "n%d startet nicht: %v"
	failTip        = "kein Bit gekippt in n%d: %v"
	failSilent     = "n%d hat binnen %v weder ohne Reparatur gestartet noch die Reparatur beendet"
	failProof      = "Beleg für Buchung %d: %v"
	failProofOther = "Beleg für Buchung %d nennt %v von Konto %v an Konto %v über %v, geschickt war %d von Konto %d an Konto %d über %d"
	failJudge      = "Prüfer: %v"
)

var roleNames = map[ledger.Role]string{
	ledger.Leader:       "Leader",
	ledger.Follower:     "Follower",
	ledger.Candidate:    "Kandidat",
	ledger.PreCandidate: "bittet um Stimmen (Pre-Vote)",
}

func standing(st *ledger.NodeStatus) string {
	if st == nil {
		return silent
	}
	return fmt.Sprintf("%s in Term %d", roleNames[st.Role], st.Term)
}

func grouped(digits string) string {
	var b strings.Builder
	for i, d := range digits {
		if i > 0 && (len(digits)-i)%3 == 0 {
			b.WriteString(" ")
		}
		b.WriteRune(d)
	}
	return b.String()
}

func num(n int) string {
	return grouped(fmt.Sprint(n))
}

func money(hundredths string) string {
	sign, digits := "", hundredths
	if rest, ok := strings.CutPrefix(digits, "-"); ok {
		sign, digits = "-", rest
	}
	digits = strings.Repeat("0", max(0, 3-len(digits))) + digits
	return sign + grouped(digits[:len(digits)-2]) + "," + digits[len(digits)-2:]
}

func secs(d time.Duration) string {
	return strings.Replace(fmt.Sprintf("%.1f", d.Seconds()), ".", ",", 1)
}
