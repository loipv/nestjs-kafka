import { Injectable, Logger } from '@nestjs/common';

export enum CircuitState {
    CLOSED = 'CLOSED',     // Normal operation
    OPEN = 'OPEN',         // DLQ blocked
    HALF_OPEN = 'HALF_OPEN', // Testing recovery
}

export interface CircuitBreakerOptions {
    failureThreshold?: number;      // Failures before opening (default: 5)
    failureRateThreshold?: number;  // Failure rate % to open (default: 50)
    resetTimeout?: number;          // ms before trying half-open (default: 30000)
    halfOpenMaxAttempts?: number;   // Attempts in half-open (default: 3)
}

interface CircuitStats {
    failures: number;
    successes: number;
    lastFailureTime: number;
    halfOpenAttempts: number;
}

/**
 * Circuit Breaker to prevent DLQ flooding
 */
@Injectable()
export class CircuitBreakerService {
    private readonly logger = new Logger(CircuitBreakerService.name);

    // Circuit state per topic
    private circuits = new Map<string, CircuitStats>();
    private states = new Map<string, CircuitState>();

    private readonly defaultOptions: Required<CircuitBreakerOptions> = {
        failureThreshold: 5,
        failureRateThreshold: 50,
        resetTimeout: 30000,
        halfOpenMaxAttempts: 3,
    };

    /**
     * Check if the circuit allows the operation
     */
    canExecute(key: string, options?: CircuitBreakerOptions): boolean {
        const opts = { ...this.defaultOptions, ...options };
        const state = this.getState(key);

        if (state === CircuitState.CLOSED) {
            return true;
        }

        if (state === CircuitState.OPEN) {
            const stats = this.circuits.get(key);
            if (stats && Date.now() - stats.lastFailureTime >= opts.resetTimeout) {
                this.setState(key, CircuitState.HALF_OPEN);
                this.logger.log(`Circuit ${key} transitioning to HALF_OPEN`);
                return true;
            }
            return false;
        }

        // HALF_OPEN - allow limited attempts
        const stats = this.circuits.get(key);
        if (stats && stats.halfOpenAttempts < opts.halfOpenMaxAttempts) {
            return true;
        }
        return false;
    }

    /**
     * Record a successful operation
     */
    recordSuccess(key: string, options?: CircuitBreakerOptions): void {
        const opts = { ...this.defaultOptions, ...options };
        const stats = this.getOrCreateStats(key);
        const state = this.getState(key);

        stats.successes++;

        if (state === CircuitState.HALF_OPEN) {
            stats.halfOpenAttempts++;
            // If we've had enough successes in half-open, close the circuit
            if (stats.halfOpenAttempts >= opts.halfOpenMaxAttempts) {
                this.reset(key);
                this.logger.log(`Circuit ${key} closed after successful recovery`);
            }
        }
    }

    /**
     * Record a failed operation
     */
    recordFailure(key: string, options?: CircuitBreakerOptions): void {
        const opts = { ...this.defaultOptions, ...options };
        const stats = this.getOrCreateStats(key);
        const state = this.getState(key);

        stats.failures++;
        stats.lastFailureTime = Date.now();

        if (state === CircuitState.HALF_OPEN) {
            // Any failure in half-open reopens the circuit
            this.setState(key, CircuitState.OPEN);
            stats.halfOpenAttempts = 0;
            this.logger.warn(`Circuit ${key} reopened after failure in HALF_OPEN`);
            return;
        }

        // Check if we should open the circuit
        const total = stats.failures + stats.successes;
        const failureRate = (stats.failures / total) * 100;

        if (
            stats.failures >= opts.failureThreshold ||
            (total >= opts.failureThreshold && failureRate >= opts.failureRateThreshold)
        ) {
            this.setState(key, CircuitState.OPEN);
            this.logger.warn(
                `Circuit ${key} opened: failures=${stats.failures}, rate=${failureRate.toFixed(1)}%`,
            );
        }
    }

    /**
     * Get current circuit state
     */
    getState(key: string): CircuitState {
        return this.states.get(key) || CircuitState.CLOSED;
    }

    /**
     * Get all circuit states
     */
    getAllStates(): Record<string, { state: CircuitState; stats: CircuitStats }> {
        const result: Record<string, { state: CircuitState; stats: CircuitStats }> = {};
        for (const [key, stats] of this.circuits) {
            result[key] = {
                state: this.getState(key),
                stats: { ...stats },
            };
        }
        return result;
    }

    /**
     * Manually reset a circuit
     */
    reset(key: string): void {
        this.states.set(key, CircuitState.CLOSED);
        this.circuits.set(key, {
            failures: 0,
            successes: 0,
            lastFailureTime: 0,
            halfOpenAttempts: 0,
        });
    }

    /**
     * Reset all circuits
     */
    resetAll(): void {
        this.circuits.clear();
        this.states.clear();
    }

    private setState(key: string, state: CircuitState): void {
        this.states.set(key, state);
    }

    private getOrCreateStats(key: string): CircuitStats {
        let stats = this.circuits.get(key);
        if (!stats) {
            stats = {
                failures: 0,
                successes: 0,
                lastFailureTime: 0,
                halfOpenAttempts: 0,
            };
            this.circuits.set(key, stats);
        }
        return stats;
    }
}
