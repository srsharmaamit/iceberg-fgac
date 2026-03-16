package com.banking.lakehouse.fgac.api.identity;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Collections;
import java.util.Objects;
import java.util.Set;

/**
 * Immutable representation of who is executing a query.
 *
 * <p>A {@code UserIdentity} carries three pieces of information:
 * <ol>
 *   <li><b>principalId</b> – the authenticated identifier (service account name,
 *       Kerberos principal, JWT subject). This is what the policy store is
 *       keyed on.</li>
 *   <li><b>roles</b> – the set of role names assigned to this principal. Roles
 *       are resolved by the {@link IdentityResolver} at session initialisation
 *       time, not per-query.</li>
 *   <li><b>attributes</b> – an opaque key/value map for jurisdiction, cost centre,
 *       or any other ABAC dimension needed by policy rules. Populated by the
 *       resolver, read by the {@code IcebergExpressionBuilder}.</li>
 * </ol>
 *
 * <p>This class is intentionally final and constructed only via its builder or
 * the static factory methods. Engine adapters must never construct a
 * {@code UserIdentity} directly — always go through the {@link IdentityResolver}
 * so that resolution logic is centralised and testable.
 */
public final class UserIdentity {

    /** Sentinel used when identity cannot be resolved. Framework applies deny-all. */
    public static final UserIdentity UNKNOWN =
            new UserIdentity("UNKNOWN", Collections.emptySet(), Collections.emptyMap());

    private final String principalId;
    private final Set<String> roles;
    private final java.util.Map<String, String> attributes;

    @JsonCreator
    private UserIdentity(
            @JsonProperty("principalId") String principalId,
            @JsonProperty("roles") Set<String> roles,
            @JsonProperty("attributes") java.util.Map<String, String> attributes) {
        this.principalId = Objects.requireNonNull(principalId, "principalId");
        this.roles       = Collections.unmodifiableSet(Objects.requireNonNull(roles, "roles"));
        this.attributes  = Collections.unmodifiableMap(Objects.requireNonNull(attributes, "attributes"));
    }

    public String getPrincipalId() { return principalId; }
    public Set<String> getRoles()  { return roles; }
    public java.util.Map<String, String> getAttributes() { return attributes; }

    public boolean hasRole(String role) { return roles.contains(role); }
    public boolean hasAnyRole(String... candidates) {
        for (String r : candidates) if (roles.contains(r)) return true;
        return false;
    }
    public String getAttribute(String key) { return attributes.get(key); }
    public String getAttribute(String key, String defaultValue) {
        return attributes.getOrDefault(key, defaultValue);
    }
    public boolean isUnknown() { return UNKNOWN.principalId.equals(principalId); }

    // ── Builder ──────────────────────────────────────────────────────────

    public static Builder builder(String principalId) {
        return new Builder(principalId);
    }

    public static final class Builder {
        private final String principalId;
        private final java.util.Set<String> roles = new java.util.LinkedHashSet<>();
        private final java.util.Map<String, String> attributes = new java.util.LinkedHashMap<>();

        private Builder(String principalId) {
            this.principalId = Objects.requireNonNull(principalId);
        }

        public Builder role(String role) {
            this.roles.add(Objects.requireNonNull(role));
            return this;
        }

        public Builder roles(java.util.Collection<String> roles) {
            this.roles.addAll(roles);
            return this;
        }

        public Builder attribute(String key, String value) {
            this.attributes.put(
                    Objects.requireNonNull(key),
                    Objects.requireNonNull(value));
            return this;
        }

        public Builder attributes(java.util.Map<String, String> attrs) {
            this.attributes.putAll(attrs);
            return this;
        }

        public UserIdentity build() {
            return new UserIdentity(principalId, roles, attributes);
        }
    }

    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (!(o instanceof UserIdentity that)) return false;
        return principalId.equals(that.principalId)
                && roles.equals(that.roles)
                && attributes.equals(that.attributes);
    }

    @Override
    public int hashCode() {
        return Objects.hash(principalId, roles, attributes);
    }

    @Override
    public String toString() {
        return "UserIdentity{principalId='" + principalId + "', roles=" + roles + '}';
    }
}