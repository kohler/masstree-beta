#ifndef SIMPLETEST_RUNNER_HH
#define SIMPLETEST_RUNNER_HH
#include "../string.hh"
#include <stdio.h>

class testrunner_base {
public:
    testrunner_base(const lcdf::String& name, const lcdf::String& type)
        : name_(name), type_(type), next_(0) {
        thehead ? thetail->next_ = this : thehead = this;
        thetail = this;
    }
    virtual ~testrunner_base() {
    }
    const lcdf::String& type() const {
        return type_;
    }
    const lcdf::String& name() const {
        return name_;
    }
    static testrunner_base* first() {
        return thehead;
    }
    static testrunner_base* find(const lcdf::String& name, const lcdf::String& type) {
        testrunner_base* t = thehead;
        while (t && (t->name_ != name || t->type_ != type))
            t = t->next_;
        return t;
    }
    static void print_names(FILE* stream, int maxcol);
private:
    static testrunner_base* thehead;
    static testrunner_base* thetail;
    lcdf::String name_;
    lcdf::String type_;
    testrunner_base* next_;
};

template <typename T>
class testrunner : public testrunner_base {
public:
    inline testrunner(const lcdf::String& name, const lcdf::String& type)
        : testrunner_base(name, type) {
    }
    static testrunner* first() {
        return static_cast<testrunner*>(testrunner_base::first());
    }
    static testrunner* find(const lcdf::String& name, const lcdf::String& type) {
        return static_cast<testrunner*>(testrunner_base::find(name, type));
    }
    virtual void run(T) = 0;
};

#define _STR(x) #x
#define STR(x) _STR(x)

// testrunner_ ## client_type ## _ ## name() : testrunner(#name) {}
#define MAKE_TESTRUNNER(client_type, name, text)                    \
    namespace {                                        \
    class testrunner_ ## client_type ## _ ## name : public testrunner<client_type> {      \
    public:                                            \
        testrunner_ ## client_type ## _ ## name() : testrunner(#name, client_type::tname()) {} \
        void run(client_type client) { text; client.finish(); } \
    }; static testrunner_ ## client_type ## _ ## name testrunner_ ## client_type ## _ ##name ## _instance; }

#endif