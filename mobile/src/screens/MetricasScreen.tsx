import React from 'react';
import {
  View, Text, StyleSheet, ScrollView,
  RefreshControl, ActivityIndicator, Dimensions,
} from 'react-native';
import { useSafeAreaInsets } from 'react-native-safe-area-context';
import { Ionicons } from '@expo/vector-icons';
import { useDesempenho } from '../hooks/useData';
import StatCard from '../components/StatCard';
import LineChart from '../components/LineChart';
import { COLORS, SPACING, RADIUS, SHADOWS } from '../theme';
import type { DesempenhoRow } from '../api/client';

const { width: SCREEN_W } = Dimensions.get('window');
const CHART_W = SCREEN_W - SPACING.md * 4;

// ── Helpers ────────────────────────────────────────────────────────────────

/** Returns color based on accuracy — green ≥ 70 %, orange ≥ 50 %, red otherwise. */
const accuracyColor = (pct: number) =>
  pct >= 70 ? COLORS.primary :
  pct >= 50 ? COLORS.warning :
  COLORS.danger;

// ── Sub-components ─────────────────────────────────────────────────────────

function TypeCell({ label, value, color, icon }: {
  label: string; value: number | null; color: string; icon: string;
}) {
  return (
    <View style={[styles.typeCell, { borderColor: `${color}40` }]}>
      <Ionicons name={icon as any} size={20} color={color} />
      <Text style={[styles.typePct, { color }]}>{value != null ? `${value}%` : '—'}</Text>
      <Text style={styles.typeLabel}>{label}</Text>
    </View>
  );
}

function ConfBox({ label, value, color, icon }: {
  label: string; value: number | null; color: string; icon: string;
}) {
  return (
    <View style={[styles.confBox, { backgroundColor: `${color}10`, borderColor: `${color}30` }]}>
      <Ionicons name={icon as any} size={18} color={color} />
      <Text style={[styles.confVal, { color }]}>{value != null ? `${value}%` : '—'}</Text>
      <Text style={styles.confLabel}>{label}</Text>
    </View>
  );
}

// ── Main Screen ────────────────────────────────────────────────────────────

export default function MetricasScreen() {
  const insets = useSafeAreaInsets();
  const { data, loading, refreshing, error, refresh } = useDesempenho();

  if (loading) {
    return <View style={styles.center}><ActivityIndicator size="large" color={COLORS.primary} /></View>;
  }

  if (error) {
    return (
      <View style={styles.center}>
        <Ionicons name="alert-circle-outline" size={40} color={COLORS.danger} />
        <Text style={styles.errorText}>{error}</Text>
      </View>
    );
  }

  // ES2023: Array.at(-1) for last element
  const latest = data?.at(-1) ?? null;

  // ES2023: Array.findLast() for best round by accuracy
  const best = data?.findLast(
    r => r.acuracia_pct === Math.max(...(data?.map(d => d.acuracia_pct) ?? [0]))
  ) ?? null;

  const acuraciaData = (data ?? []).map(r => ({ x: r.rodada, y: r.acuracia_pct ?? 0 }));
  const media5rData = (data ?? [])
    .filter(r => r.acuracia_media_5r != null)
    .map(r => ({ x: r.rodada, y: r.acuracia_media_5r! }));

  // Last row with type breakdown data
  const latestType = data?.findLast(
    r => r.acuracia_casa_pct != null || r.acuracia_empate_pct != null || r.acuracia_visitante_pct != null
  ) ?? null;

  // Last 10 rounds reversed (ES2023: toReversed is non-mutating)
  const recentRounds = (data ?? []).toReversed().slice(0, 10);

  return (
    <ScrollView
      style={styles.container}
      contentContainerStyle={[styles.content, { paddingTop: insets.top + SPACING.md }]}
      refreshControl={
        <RefreshControl
          refreshing={refreshing}
          onRefresh={refresh}
          tintColor={COLORS.primary}
          colors={[COLORS.primary]}
        />
      }
      showsVerticalScrollIndicator={false}
    >
      <View style={styles.pageHeader}>
        <Text style={styles.title}>Métricas do Modelo</Text>
        <Text style={styles.subtitle}>Desempenho histórico das previsões</Text>
      </View>

      {/* KPIs */}
      {latest && (
        <View style={styles.kpiRow}>
          <StatCard
            label="Acurácia Geral"
            value={`${latest.acuracia_geral_pct}%`}
            icon="analytics"
            color={COLORS.primary}
            sublabel="acumulada"
          />
          <View style={{ width: SPACING.sm }} />
          <StatCard
            label="Melhor Rodada"
            value={best ? `${best.acuracia_pct}%` : '—'}
            icon="trophy"
            color={COLORS.gold}
            sublabel={best ? `Rd ${best.rodada}` : ''}
          />
          <View style={{ width: SPACING.sm }} />
          <StatCard
            label="Total Jogos"
            value={latest.total_jogos_acumulado}
            icon="football"
            color={COLORS.info}
          />
        </View>
      )}

      {/* Accuracy chart */}
      {acuraciaData.length > 1 && (
        <View style={[styles.card, SHADOWS.card]}>
          <Text style={styles.cardTitle}>Acurácia por Rodada</Text>
          <LineChart
            width={CHART_W}
            height={220}
            yFormat={(v: number) => `${Math.round(v)}%`}
            xLabel="Rodada"
            series={[
              { data: acuraciaData, color: COLORS.primary, label: 'Por rodada' },
              ...(media5rData.length > 1
                ? [{ data: media5rData, color: COLORS.gold, dashed: true, label: 'Média 5R' }]
                : []),
            ]}
          />
        </View>
      )}

      {/* Type breakdown */}
      {latestType && (
        <View style={[styles.card, SHADOWS.card]}>
          <Text style={styles.cardTitle}>Acurácia por Resultado</Text>
          <Text style={styles.cardSubtitle}>Rodada {latestType.rodada}</Text>
          <View style={styles.typeGrid}>
            <TypeCell label="Vitória Casa"   value={latestType.acuracia_casa_pct}      color={COLORS.primary} icon="home"     />
            <TypeCell label="Empate"         value={latestType.acuracia_empate_pct}    color={COLORS.warning} icon="remove"   />
            <TypeCell label="Vit. Visitante" value={latestType.acuracia_visitante_pct} color={COLORS.info}    icon="airplane" />
          </View>
        </View>
      )}

      {/* Confidence analysis */}
      {latest && (
        <View style={[styles.card, SHADOWS.card]}>
          <Text style={styles.cardTitle}>Análise de Confiança</Text>
          <Text style={styles.cardSubtitle}>Média de confiança: acertos vs erros</Text>
          <View style={styles.confRow}>
            <ConfBox label="Acertos" value={latest.conf_media_acerto} color={COLORS.primary} icon="checkmark-circle" />
            <ConfBox label="Erros"   value={latest.conf_media_erro}   color={COLORS.danger}  icon="close-circle"     />
          </View>
          {latest.conf_media_acerto != null && latest.conf_media_erro != null && (
            <View style={styles.diffBox}>
              <Text style={styles.diffText}>
                Diferença de{' '}
                <Text style={{ color: COLORS.primary, fontWeight: '700' }}>
                  {(latest.conf_media_acerto - latest.conf_media_erro).toFixed(1)}%
                </Text>
                {' '}— modelo bem calibrado
              </Text>
            </View>
          )}
        </View>
      )}

      {/* Per-round table */}
      <View style={[styles.card, SHADOWS.card, { padding: 0, overflow: 'hidden' }]}>
        <View style={styles.tableHeader}>
          <Text style={styles.cardTitle}>Histórico por Rodada</Text>
        </View>
        <View style={styles.tableColRow}>
          {(['Rd', 'Jogos', 'Acertos', 'Acurácia', 'Média 5R'] as const).map(h => (
            <Text key={h} style={styles.tableColH}>{h}</Text>
          ))}
        </View>
        {recentRounds.map((r: DesempenhoRow, i: number) => (
          <View key={r.rodada} style={[styles.tableRow, i % 2 === 0 && styles.tableRowEven]}>
            <Text style={styles.tableCell}>{r.rodada}</Text>
            <Text style={styles.tableCell}>{r.total_jogos}</Text>
            <Text style={styles.tableCell}>{r.acertos}</Text>
            <Text style={[styles.tableCell, { color: accuracyColor(r.acuracia_pct) }]}>
              {r.acuracia_pct}%
            </Text>
            <Text style={styles.tableCell}>
              {r.acuracia_media_5r != null ? `${r.acuracia_media_5r}%` : '—'}
            </Text>
          </View>
        ))}
      </View>
    </ScrollView>
  );
}

const styles = StyleSheet.create({
  container: { flex: 1, backgroundColor: COLORS.bg },
  content:   { padding: SPACING.md, gap: SPACING.md, paddingBottom: SPACING.xl },
  center: {
    flex: 1, backgroundColor: COLORS.bg,
    justifyContent: 'center', alignItems: 'center', gap: SPACING.sm,
  },
  pageHeader: { marginBottom: SPACING.sm },
  title:    { color: COLORS.text, fontSize: 26, fontWeight: '800', letterSpacing: -0.5 },
  subtitle: { color: COLORS.textSecondary, fontSize: 13, marginTop: 2 },
  kpiRow:   { flexDirection: 'row' },
  card: {
    backgroundColor: COLORS.bgCard,
    borderRadius: RADIUS.lg,
    borderWidth: 1,
    borderColor: COLORS.bgBorder,
    padding: SPACING.md,
    gap: SPACING.sm,
  },
  cardTitle:    { color: COLORS.text, fontSize: 15, fontWeight: '700' },
  cardSubtitle: { color: COLORS.textMuted, fontSize: 12 },
  typeGrid:     { flexDirection: 'row', gap: SPACING.sm, marginTop: SPACING.xs },
  typeCell: {
    flex: 1, alignItems: 'center', gap: 4,
    backgroundColor: COLORS.bgCardAlt,
    borderWidth: 1, borderRadius: RADIUS.md, paddingVertical: SPACING.sm,
  },
  typePct:   { fontSize: 18, fontWeight: '800' },
  typeLabel: { color: COLORS.textMuted, fontSize: 10, textAlign: 'center' },
  confRow:   { flexDirection: 'row', gap: SPACING.sm },
  confBox: {
    flex: 1, flexDirection: 'row', alignItems: 'center', gap: SPACING.sm,
    borderWidth: 1, borderRadius: RADIUS.md, padding: SPACING.sm,
  },
  confVal:   { fontSize: 20, fontWeight: '800' },
  confLabel: { color: COLORS.textSecondary, fontSize: 12 },
  diffBox: {
    backgroundColor: 'rgba(0,210,106,0.07)', borderRadius: RADIUS.sm,
    padding: SPACING.sm, borderWidth: 1, borderColor: 'rgba(0,210,106,0.2)',
  },
  diffText: { color: COLORS.textSecondary, fontSize: 12, textAlign: 'center' },
  tableHeader: {
    padding: SPACING.md, paddingBottom: SPACING.sm,
    borderBottomWidth: 1, borderBottomColor: COLORS.bgBorder,
  },
  tableColRow: {
    flexDirection: 'row', paddingHorizontal: SPACING.md,
    paddingVertical: 6, backgroundColor: COLORS.bgCardAlt,
  },
  tableColH: {
    flex: 1, color: COLORS.textMuted, fontSize: 10,
    fontWeight: '700', textTransform: 'uppercase',
    letterSpacing: 0.5, textAlign: 'center',
  },
  tableRow: {
    flexDirection: 'row', paddingHorizontal: SPACING.md,
    paddingVertical: 10, borderBottomWidth: 1,
    borderBottomColor: `${COLORS.bgBorder}60`,
  },
  tableRowEven: { backgroundColor: COLORS.bgCardAlt },
  tableCell: { flex: 1, color: COLORS.textSecondary, fontSize: 13, textAlign: 'center' },
  errorText: { color: COLORS.danger, fontSize: 13, textAlign: 'center' },
});
