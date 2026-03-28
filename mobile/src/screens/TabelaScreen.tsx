import React from 'react';
import {
  View, Text, FlatList, StyleSheet,
  RefreshControl, ActivityIndicator,
} from 'react-native';
import { useSafeAreaInsets } from 'react-native-safe-area-context';
import { Ionicons } from '@expo/vector-icons';
import { useClassificacao } from '../hooks/useData';
import StandingsRow from '../components/StandingsRow';
import { COLORS, SPACING, RADIUS } from '../theme';
import type { ClassificacaoRow } from '../api/client';

const LEGEND = [
  { label: 'Libertadores (fase grupos)', color: COLORS.libertadores },
  { label: 'Libertadores (pré-fase)',    color: COLORS.preLibertadores },
  { label: 'Sul-Americana',              color: COLORS.sulAmericana },
  { label: 'Rebaixamento',               color: COLORS.danger },
] as const;

const COL_HEADERS = ['Pts', 'J', 'V', 'E', 'D', 'SG', '%'] as const;

export default function TabelaScreen() {
  const insets = useSafeAreaInsets();
  const { data, loading, refreshing, error, refresh } = useClassificacao();

  // Use ES2023 toSorted for immutable sort
  const sorted = data?.toSorted((a, b) => a.posicao - b.posicao) ?? [];

  const renderHeader = () => (
    <View>
      <View style={[styles.header, { paddingTop: insets.top + SPACING.md }]}>
        <Text style={styles.title}>Classificação</Text>
        <Text style={styles.subtitle}>Campeonato Brasileiro Série A 2026</Text>
      </View>

      <View style={styles.colHeaders}>
        <View style={{ width: 3, marginRight: 6 }} />
        <Text style={[styles.colH, { width: 24 }]}>#</Text>
        <View style={{ width: 22, marginHorizontal: 6 }} />
        <Text style={[styles.colH, { flex: 1 }]}>Clube</Text>
        {COL_HEADERS.map(h => (
          <Text key={h} style={[styles.colH, { width: 30, textAlign: 'center' }]}>{h}</Text>
        ))}
      </View>
    </View>
  );

  if (loading) {
    return (
      <View style={styles.center}>
        <ActivityIndicator size="large" color={COLORS.primary} />
      </View>
    );
  }

  if (error) {
    return (
      <View style={styles.center}>
        <Ionicons name="alert-circle-outline" size={40} color={COLORS.danger} />
        <Text style={styles.errorText}>{error}</Text>
      </View>
    );
  }

  return (
    <View style={styles.container}>
      <FlatList<ClassificacaoRow>
        data={sorted}
        keyExtractor={item => String(item.posicao)}
        renderItem={({ item, index }) => <StandingsRow row={item} index={index} />}
        ListHeaderComponent={renderHeader}
        ListFooterComponent={<LegendFooter />}
        contentContainerStyle={styles.list}
        refreshControl={
          <RefreshControl
            refreshing={refreshing}
            onRefresh={refresh}
            tintColor={COLORS.primary}
            colors={[COLORS.primary]}
          />
        }
        showsVerticalScrollIndicator={false}
      />
    </View>
  );
}

function LegendFooter() {
  return (
    <View style={styles.legend}>
      {LEGEND.map(l => (
        <View key={l.label} style={styles.legendItem}>
          <View style={[styles.legendDot, { backgroundColor: l.color }]} />
          <Text style={styles.legendText}>{l.label}</Text>
        </View>
      ))}
    </View>
  );
}

const styles = StyleSheet.create({
  container: { flex: 1, backgroundColor: COLORS.bg },
  list:      { paddingBottom: SPACING.xl },
  header: {
    paddingHorizontal: SPACING.md,
    paddingBottom: SPACING.lg,
    backgroundColor: COLORS.bg,
  },
  title: { color: COLORS.text, fontSize: 26, fontWeight: '800', letterSpacing: -0.5 },
  subtitle: { color: COLORS.textSecondary, fontSize: 13, marginTop: 2 },
  colHeaders: {
    flexDirection: 'row', alignItems: 'center',
    paddingVertical: SPACING.xs, paddingRight: SPACING.md,
    backgroundColor: COLORS.bgCardAlt,
    borderBottomWidth: 1, borderBottomColor: COLORS.bgBorder,
    marginBottom: 2,
  },
  colH: {
    color: COLORS.textMuted, fontSize: 10,
    fontWeight: '700', letterSpacing: 0.5, textTransform: 'uppercase',
  },
  center: {
    flex: 1, backgroundColor: COLORS.bg,
    justifyContent: 'center', alignItems: 'center', gap: SPACING.sm,
  },
  errorText: { color: COLORS.danger, fontSize: 13, textAlign: 'center' },
  legend: {
    padding: SPACING.md, gap: SPACING.xs,
    borderTopWidth: 1, borderTopColor: COLORS.bgBorder, marginTop: SPACING.md,
  },
  legendItem: { flexDirection: 'row', alignItems: 'center', gap: SPACING.sm },
  legendDot: { width: 8, height: 8, borderRadius: RADIUS.full },
  legendText: { color: COLORS.textSecondary, fontSize: 12 },
});
